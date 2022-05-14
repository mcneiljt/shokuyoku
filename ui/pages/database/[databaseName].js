import styles from "../../styles/Home.module.css";
import { useRouter } from 'next/router'

export async function getServerSideProps({params}) {
    const { databaseName } = params
    const types = await fetch("http://localhost:3004/types").then(response => response.json())
    const deltaEventTypes = await fetch("http://localhost:3004/deltas/event_type").then(response => response.json())
    const eventTypes = await fetch(`http://localhost:3004/schemas/${databaseName}`).then(response => response.json())

    return {
        props: {types, deltaEventTypes, eventTypes}, // will be passed to the page component as props
    }
}


export default function Home({eventTypes, deltaEventTypes}) {
    const router = useRouter()
    const { databaseName } = router.query

    return (<div>

    <a href={`/database/${databaseName}/create_table`}>Create Table</a>

            <ul>
                {deltaEventTypes.map(function(deltaEventType){
                    return <li><a href={`/database/${databaseName}/event_type/${deltaEventType.name}`}>{deltaEventType.name}</a></li>
                })}
            </ul>
            Event Types
            <ul>
                {eventTypes.map(function(eventType){
                    return <li><a href={`/database/${databaseName}/event_type/${eventType}`}>{eventType}</a></li>
                })}
            </ul>
    </div>
    )};
