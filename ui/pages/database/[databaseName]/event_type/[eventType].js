import _ from 'lodash'
import {useRouter} from "next/router";
import React, { useState } from 'react';

export async function getServerSideProps({params}) {
    const { databaseName, eventType } = params
    const types = await fetch("http://localhost:3004/types").then(response => response.json())
    const deltas = await fetch(`http://localhost:3004/deltas/event_type/${eventType}`).then(response => response.json())
    const eventTypeObj = await fetch(`http://localhost:3004/schemas/${databaseName}/${eventType}`).then(response => response.json())

    return {
        props: {types, deltas, eventType: eventTypeObj}, // will be passed to the page component as props
    }
}

export default function EventTypeEditor({deltas, eventType, types}) {

    const router = useRouter()
    const eventTypeName = router.query.eventType;
    const { databaseName } = router.query

    const [tableName, setTableName] = useState(eventTypeName);

    const [partitionType, setPartitionType] = useState("date");
    const [partitionKey, setPartitionKey] = useState("date");
    const [location, setLocation] = useState("s3a://analytics/{table_name}");

    const columnMap = {};
    const columnsRaw = [];
    (eventType ? eventType.sd.cols : []).forEach(function(eventTypeColumn){
       // if(!columnMap[eventTypeColumn.name]) {
            const obj = {
                name:eventTypeColumn.name,
                //
                type: eventTypeColumn.type,
            };
        columnsRaw.push(obj)
        columnMap[eventTypeColumn.name] = obj;
  //  }
    })

    deltas.forEach(function(delta){
        if(!columnMap[delta.name.name]) {
            const obj = {
                name:delta.name.name,

                type: delta.type,
                error: true,
                lastError: delta.lastError
            };
            columnMap[delta.name.name] = obj;
            columnsRaw.push(obj)
        }
    });
   // const columnsTmp = _.values(columnMap).sort((a,b)=>a.name.localeCompare(b.name));
    const [columns, setColumns] = useState(columnsRaw);

    function saveTable() {
        const obj = {
            name: tableName,
            partitionedBy:[{
                name: partitionKey,
                type: partitionType
            }],
            columns: columns.map(function(c){
                return {name: c.name, type: c.type}
            }),
            location: location.replace("{table_name}", tableName)
        }
        console.log(JSON.stringify(obj))

        fetch(`/schemas/${databaseName}/${encodeURIComponent(tableName)}`, {
            method: eventType ? 'PUT' : 'POST', // or 'PUT'
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(obj),
        })
    }


    return <div><h2>Event Type</h2>
        <div>
            <h3>Name: {tableName}</h3>

            <h2>Partitioned By</h2>

            <select onChange={function(e){
                setPartitionType(e.target.value);
            }}>{types.map(function(type){
                return <option selected={type===partitionType}>{type}</option>
            })}</select>

            <input type={'text'} value={partitionKey} onChange={e => setPartitionKey(e.target.value)} />

            <h2>Location</h2>
            <input type={'text'} value={location} onChange={e => setLocation(e.target.value)}/>


            <h3>Columns</h3>
            <table>
                <thead>
                <tr>
                    <th>D</th>
                    <th>Name</th>
                    <th>Type</th>
                    <th>Note</th>
                </tr>
                </thead><tbody>
            {columns.map(function(col){
                return (<tr key={col.name}>
                    <td>Delete</td>
                    <td style={{color: col.error ?'red' : null}}>{col.name}</td>
                    <td>{col.type}</td>
                    <td>{col.lastError ? `Last Error: ${col.lastError}` : ''}</td>
                </tr>);
            }
            )}</tbody>
            </table>

        </div>
        <button onClick={saveTable} >Save</button>

    </div>
}
