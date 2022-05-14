import React, { useState, useEffect } from 'react';

export default function Databases() {
  const [databases, setDatabases] = useState([]);

  useEffect(() => {
    fetch('/schemas').then((response) => response.json()).then((d) => {
      setDatabases(d);
    });
  }, []);

  return (
    <div>
      <ul>
        {databases.map((databaseName) => <li key={databaseName}><a href={`/database/${encodeURIComponent(databaseName)}`}>{databaseName}</a></li>)}
      </ul>
    </div>
  );
}
