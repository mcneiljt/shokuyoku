import _ from 'lodash'

export async function getServerSideProps({params}) {
    const { databaseName, eventType } = params
    const types = await fetch("http://localhost:3004/types").then(response => response.json())
    const deltas = await fetch(`http://localhost:3004/deltas/event_type/${eventType}`).then(response => response.json())
    const eventTypeObj = await fetch(`http://localhost:3004/schemas/${databaseName}/${eventType}`).then(response => response.json())

    return {
        props: {types, deltas, eventType: eventTypeObj}, // will be passed to the page component as props
    }
}

export default function EventTypeEditor({deltas,eventType}) {

    const columnMap = {};
    (eventType || []).forEach(function(eventTypeColumn){
        //columns[eventTypeColumn.]
    })

    deltas.forEach(function(delta){
        if(!columnMap[delta.name.name]) {
            columnMap[delta.name.name] = {
                name:delta.name.name,

                type: delta.type,
                error: true
            }
        }
    });

    const columns = _.values(columnMap).sort((a,b)=>a.name.localeCompare(b.name));

    return <div><h2>Event Type</h2>
        <div>
            {JSON.stringify(eventType)}

            <table><tbody>
            {columns.map(function(col){
                return <tr key={col.name}><td style={{color: col.error ?'red' : null}}>{col.name}</td><td>{col.type}</td></tr>
            }
            )}</tbody>
            </table>
        </div>
    </div>
}
