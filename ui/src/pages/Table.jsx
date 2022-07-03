import React, {useEffect, useState, useCallback} from 'react';
import BPromise from 'bluebird';
import {useParams, useNavigate} from 'react-router-dom';
import Button from '@mui/material/Button';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';
import TextField from '@mui/material/TextField';
import MenuItem from '@mui/material/MenuItem';
import Select from '@mui/material/Select';
import IconButton from '@mui/material/IconButton';
import MoreVertIcon from '@mui/icons-material/MoreVert';
import Menu from '@mui/material/Menu';
import Dialog from '@mui/material/Dialog';
import DialogActions from '@mui/material/DialogActions';
import DialogContent from '@mui/material/DialogContent';
import DialogTitle from '@mui/material/DialogTitle';
import PropTypes from 'prop-types';

const ITEM_HEIGHT = 20;

export default function EventTypeEditor({newEventType}) {
  const {databaseName, eventTypeName} = useParams();
  const navigate = useNavigate();

  const [types, setTypes] = useState([]);
  const [eventType, setEventType] = useState([]);
  const [columns, setColumns] = useState([]);
  const [tableName, setTableName] = useState(eventTypeName);
  const [partitionType, setPartitionType] = useState('date');
  const [partitionKey, setPartitionKey] = useState('date');
  const [location, setLocation] = useState('s3a://analytics/{table_name}');
  const [deleteColumn, setDeleteColumn] = useState(null);
  const [menuColumn, setMenuColumn] = useState(null);
  const [maybeDeleteTable, setMaybeDeleteTable] = useState(false);

  useEffect(() => {
    BPromise.all([
      fetch('/types').then((response) => response.json()),
      fetch(`/deltas/event_type/${eventTypeName}`).then((response) => response.json()),
      fetch(`/schemas/${databaseName}/table/${eventTypeName}`).then((response) => response.json()),
    ]).then(([typesTmp, deltasTmp, eventTypeTmp]) => {
      setTypes(typesTmp);
      setEventType(eventTypeTmp);

      if (eventType) {
        setPartitionKey(eventTypeTmp.partitionKeys[0].name);
        setPartitionType(eventTypeTmp.partitionKeys[0].type);
        setLocation(eventTypeTmp.sd.location);
      }

      const columnMap = {};
      const columnsRaw = [];
      (eventType ? eventTypeTmp.sd.cols : []).forEach((eventTypeColumn) => {
        const obj = {
          name: eventTypeColumn.name,
          type: eventTypeColumn.type,
        };
        columnsRaw.push(obj);
        columnMap[eventTypeColumn.name] = obj;
      });

      deltasTmp.forEach((delta) => {
        if (!columnMap[delta.name.name]) {
          const obj = {
            name: delta.name.name,

            type: delta.type,
            error: true,
            lastError: delta.lastError,
          };
          columnMap[delta.name.name] = obj;
          columnsRaw.push(obj);
        }
      });
      setColumns(columnsRaw);
    });
  }, [setColumns]);

  const addColumn = useCallback(() => {
    columns.push({name: '', type: 'string', editing: true});
    setColumns([].concat(columns));
  }, [columns]);

  const deleteTable = useCallback(async () => {
    await fetch(`/schemas/${databaseName}/table/${encodeURIComponent(tableName)}`, {
      method: 'delete', // or 'PUT'
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({}),
    });
    navigate(`/database/${databaseName}`);
  });

  const saveTable = useCallback(() => {
    const obj = {
      name: tableName,
      partitionedBy: [
        {
          name: partitionKey,
          type: partitionType,
        },
      ],
      columns: columns.map((c) => ({name: c.name, type: c.type})),
      location: location.replace('{table_name}', tableName),
    };

    // TODO: create table
    fetch(`/schemas/${databaseName}/${encodeURIComponent(tableName)}`, {
      // event type existing means it was fetched so update, otherwise its null so create
      method: eventType ? 'PUT' : 'POST', // or 'PUT'
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(obj),
    });
  }, [tableName, partitionKey, partitionType, columns, location, eventType]);

  const [anchorEl, setAnchorEl] = useState(null);
  return (
    <div>
      {newEventType ? null : (
        <Button
          style={{float: 'right'}}
          variant="text"
          onClick={() => {
            setMaybeDeleteTable(true);
          }}
        >
          Delete
        </Button>
      )}
      {maybeDeleteTable ? (
        <Dialog
          open={() => {
            console.log('dop');
          }}
          onClose={() => {
            setMaybeDeleteTable(null);
          }}
          aria-labelledby="alert-dialog-title"
          aria-describedby="alert-dialog-description"
        >
          <DialogTitle id="alert-dialog-title">Delete {tableName}?</DialogTitle>
          <DialogContent />
          <DialogActions>
            <Button
              onClick={() => {
                setMaybeDeleteTable(null);
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={() => {
                setMaybeDeleteTable(null);
                deleteTable();
              }}
              autoFocus
            >
              Delete
            </Button>
          </DialogActions>
        </Dialog>
      ) : null}
      {deleteColumn !== null ? (
        <Dialog
          open={() => {
            console.log('dop');
          }}
          onClose={() => {
            setDeleteColumn(null);
          }}
          aria-labelledby="alert-dialog-title"
          aria-describedby="alert-dialog-description"
        >
          <DialogTitle id="alert-dialog-title">
            Delete column: {columns[deleteColumn].name}?
          </DialogTitle>
          <DialogContent />
          <DialogActions>
            <Button
              onClick={() => {
                setDeleteColumn(null);
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={() => {
                columns.splice(deleteColumn, 1);
                setColumns(columns);
                setDeleteColumn(null);
              }}
              autoFocus
            >
              Delete
            </Button>
          </DialogActions>
        </Dialog>
      ) : null}
      <h2>Event Type</h2>
      <div>
        <h3>
          Name:
          {newEventType ? (
            <TextField
              onChange={(evt) => {
                setTableName(evt.target.value);
              }}
              id="standard-basic"
              label="Table Name"
              variant="standard"
              value={tableName}
            />
          ) : (
            tableName
          )}
        </h3>

        <h2>Partitioned By</h2>

        <Select
          value={partitionType}
          label="Partition Type"
          onChange={(e) => {
            setPartitionType(e.target.value);
          }}
        >
          {types.map((type) => (
            <MenuItem value={type}>{type}</MenuItem>
          ))}
        </Select>

        <TextField
          onChange={(evt) => {
            setPartitionKey(evt.target.value);
          }}
          id="standard-basic"
          label="Column Name"
          variant="standard"
          value={partitionKey}
        />

        <h2>Location</h2>
        <input type="text" value={location} onChange={(e) => setLocation(e.target.value)} />

        <h3>Columns</h3>
        <TableContainer component={Paper}>
          <Table sx={{minWidth: 650}} aria-label="simple table">
            <TableHead>
              <TableRow>
                <TableCell align="left">Name</TableCell>
                <TableCell align="left">Type</TableCell>
                <TableCell align="left">Note</TableCell>
                <TableCell align="left">Stuff</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {columns.map((col, idx) => {
                const myId = idx;
                const open = Boolean(anchorEl);
                const handleClick = (event) => {
                  setMenuColumn(idx);
                  setAnchorEl(event.currentTarget);
                };
                const handleClose = () => {
                  setAnchorEl(null);
                };
                const handleDelete = () => {
                  console.log(`handle dele2t2e: ${myId} ${JSON.stringify(col)}`);
                  setDeleteColumn(myId);
                  setAnchorEl(null);
                };
                return (
                  <TableRow>
                    <TableCell style={{color: col.error ? 'red' : null}}>
                      {col.editing ? (
                        <TextField
                          onChange={(evt) => {
                            columns[idx].name = evt.target.value;
                            setColumns(columns);
                          }}
                          id="standard-basic"
                          label="Column Name"
                          variant="standard"
                        />
                      ) : (
                        <div>{col.name}</div>
                      )}
                    </TableCell>
                    <TableCell>
                      {col.editing ? (
                        <Select
                          value={col.type}
                          label="Type"
                          onChange={(evt) => {
                            console.log('S2ELECT hange2');
                            columns[idx].type = evt.target.value;
                            //  setColumns(columns);
                            setColumns([].concat(columns));
                          }}
                        >
                          {types.map((type) => (
                            <MenuItem value={type}>{type}</MenuItem>
                          ))}
                        </Select>
                      ) : (
                        <div>{col.type}</div>
                      )}
                    </TableCell>
                    <TableCell>{col.lastError ? `Las2t  Error: ${col.lastError}` : ''}</TableCell>
                    <TableCell>
                      <IconButton
                        aria-label="more"
                        id="long-button"
                        aria-controls={open ? 'long-menu' : undefined}
                        aria-expanded={open ? 'true' : undefined}
                        aria-haspopup="true"
                        onClick={(evt) => {
                          handleClick(evt);
                        }}
                      >
                        <MoreVertIcon />
                      </IconButton>
                      {menuColumn === idx ? (
                        <Menu
                          id="long-menu"
                          MenuListProps={{
                            'aria-labelledby': 'long-button',
                          }}
                          anchorEl={anchorEl}
                          open={open}
                          onClose={handleClose}
                          PaperProps={{
                            style: {
                              maxHeight: ITEM_HEIGHT * 4.5,
                              width: '20ch',
                            },
                          }}
                        >
                          <MenuItem key="delete" onClick={handleDelete}>
                            delete
                          </MenuItem>
                        </Menu>
                      ) : null}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
      </div>
      <Button variant="text" onClick={addColumn}>
        Add Column
      </Button>
      <Button variant="text" onClick={saveTable}>
        save
      </Button>
    </div>
  );
}

EventTypeEditor.propTypes = {
  newEventType: PropTypes.bool,
};
EventTypeEditor.defaultProps = {
  newEventType: null,
};
