import React, { useState, useEffect } from 'react';
import {
  useParams,
} from 'react-router-dom';

export default function Database() {
  const { databaseName } = useParams();

  // const [types, setTypes] = useState([]);
  const [eventTypes, setEventTypes] = useState([]);
  const [deltaEventTypes, setDeltaEventTypes] = useState({});

  useEffect(() => {
    // fetch('/types').then((response) => response.json()).then(setTypes);
    fetch('/deltas/event_type').then((response) => response.json()).then((delta) => {
      const d = {};
      delta.forEach((item) => {
        d[item.name] = item;
      });
      setDeltaEventTypes(d);
    });
    fetch(`/schemas/${databaseName}`).then((response) => response.json()).then(setEventTypes);
  }, []);

  return (
    <div>
      <h1>{databaseName}</h1>
      <h2>Event Types</h2>
      <a href={`/database/${databaseName}/create_table`}>Create Table</a>

      <ul>
        {(eventTypes || []).map((eventType) => (
          <li>
            <a href={`/database/${databaseName}/event_type/${eventType}`}>
              {eventType}
              {' '}
              {deltaEventTypes[eventType] ? (
                <span style={{ color: 'red' }}>
                  Last error:
                  {deltaEventTypes[eventType].lastError}
                </span>
              ) : ''}
            </a>
          </li>
        ))}
      </ul>
    </div>
  );
}
