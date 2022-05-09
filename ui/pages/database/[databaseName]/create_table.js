import styles from "../../../styles/Home.module.css";
import { useRouter } from 'next/router'
import React, { useState } from 'react';

export async function getServerSideProps(context) {
    const types = await fetch("http://localhost:3004/types").then(response => response.json())

    return {
        props: {types}, // will be passed to the page component as props
    }
}


export default function Home({types}) {
    const router = useRouter()
    const { databaseName } = router.query

    const [tableName, setTableName] = useState("new_table");

    const [partitionType, setPartitionType] = useState("date");
    const [partitionKey, setPartitionKey] = useState("date");
    const [location, setLocation] = useState("s3a://analytics/{table_name}");

    function saveTable() {
        const obj = {
            name: tableName,
            partitionedBy:[{
                columnName: partitionKey,
                type: partitionType
            }],
            location: location.replace("{table_name}", tableName)
        }
        console.log(JSON.stringify(obj))

        fetch(`/schemas/${databaseName}/${encodeURIComponent(tableName)}`, {
            method: 'POST', // or 'PUT'
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(obj),
        })
    }

    return (<div>
            <button onClick={saveTable} >Save</button>
            <input type={'text'} value={tableName} onChange={e => setTableName(e.target.value)} />

            <h2>Partitioned By</h2>

            <select onChange={function(e){
                setPartitionType(e.target.value);
            }}>{types.map(function(type){
                return <option selected={type===partitionType}>{type}</option>
            })}</select>

            <input type={'text'} value={partitionKey} onChange={e => setPartitionKey(e.target.value)} />

            <h2>Location</h2>
            <input type={'text'} value={location} onChange={e => setLocation(e.target.value)}/>

            <h2>Columns</h2>
            <button onClick={saveTable} >Save</button>
        </div>
    )};
