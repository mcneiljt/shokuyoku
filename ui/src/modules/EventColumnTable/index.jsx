import React, {useMemo, useState, useEffect} from 'react';
import {useTable, useFlexLayout, useRowSelect} from 'react-table';
import {Container, Text, Button, Tooltip} from 'components/ui';
import {EditableCell} from './components/EditableCell';
import {IndeterminateCheckbox} from './components/IndeterminateCheckbox';

import styled from 'styled-components';
import css from '@styled-system/css';

// TODO: move these styles
const HeaderGroupContainer = styled(Container)`
  text-transform: uppercase;
  height: 50px;
  gap: 1rem;
  border-bottom: 1px solid;
  ${css({
    borderColor: 'background',
  })}
`;
const HeaderContainer = styled(Container)``;
const BodyContainer = styled(Container)``;
const RowContainer = styled(Container)`
  gap: 1rem;
  height: 40px;
  align-items: center;
  transition: all 0.1s ease;
  border: 1px solid transparent;
  ${({theme}) => {
    return css({
      borderBottom: `1px solid ${theme.colors.background}`,
      ':hover': {
        border: '1px solid',
        borderColor: 'navColor',
      },
    });
  }}
`;

const CellWrapper = styled(Container)`
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  border-right: 1px solid;
  overflow: visible;
  ${css({
    borderColor: 'background',
  })}
`;

export const EventColumnTable = ({
  tableData,
  onBlurRow,
  diffData,
  onAddEventColumn,
  onSelectEventColumn,
  selectedEventColumns,
  types,
}) => {
  const [maxId, setMaxId] = useState(-1);

  const data = useMemo(() => {
    return tableData;
  }, [tableData]);

  // TODO: move to config
  // think of comment as a "description"?
  const columns = useMemo(() => {
    return [
      {
        Header: 'name',
        accessor: 'name',
        width: 100,
      },
      {
        Header: 'type',
        accessor: 'type',
        width: 50,
      },
      {
        Header: 'comment',
        accessor: 'comment',
        Cell: EditableCell,
        width: 150,
      },
    ];
  }, []);

  const defaultColumn = {
    Cell: EditableCell,
  };

  useEffect(() => {
    const ids = tableData.map((data) => data.id).sort();
    const maxId = ids[ids.length - 1];

    setMaxId(maxId);
  }, [tableData]);

  const tableInstance = useTable(
    {
      columns,
      data,
      defaultColumn,
      onBlurRow,
      initialState: {selectedRowIds: selectedEventColumns},
      types,
    },
    useFlexLayout,
    useRowSelect,
    (hooks) => {
      hooks.visibleColumns.push((columns) => {
        return [
          {
            id: 'selection',
            Header: (
              <Tooltip content="Selected rows will be deleted upon save.">
                <Container style={{cursor: 'help'}} textAlign="center">
                  🗑️
                </Container>
              </Tooltip>
            ),
            Cell: ({row}) => {
              return (
                <Container width="100%" textAlign="center" justifySelf="center">
                  <IndeterminateCheckbox {...row.getToggleRowSelectedProps()} />
                </Container>
              );
            },
            width: 10,
          },
          ...columns,
        ];
      });
    }
  );
  const {
    getTableProps,
    getTableBodyProps,
    headerGroups,
    rows,
    prepareRow,
    state: {selectedRowIds},
  } = tableInstance;

  const handleAddEventColumn = () => {
    onAddEventColumn(maxId);
  };

  useEffect(() => {
    onSelectEventColumn(selectedRowIds);
  }, [selectedRowIds]);

  const debug = false;

  return (
    <Container
      border="1px solid"
      borderColor="borderColor"
      paddingX="small"
      paddingTop="tiny"
      paddingBottom="small"
      borderRadius="small"
      backgroundColor="cardBackground"
    >
      <Container display="flex" gap="medium" alignItems="center">
        <Text color="headingColor" heading={3}>
          {'Columns'.toUpperCase()}
        </Text>
        <Button onClick={handleAddEventColumn}>add event column</Button>
      </Container>
      {debug && (
        <>
          <Container>{JSON.stringify(diffData)}</Container>
          <Container>{JSON.stringify(tableData)}</Container>
          <Container>Max ID: {maxId}</Container>
          <Container>{JSON.stringify(selectedRowIds)}</Container>
        </>
      )}

      <Container
        border="1px solid"
        borderRadius="medium"
        marginTop="small"
        bg="white"
        borderColor="background"
        width="100%"
        display="flex"
        flexDirection="column"
        alignItems="stretch"
        backgroundColor="background"
        {...getTableProps({style: {overflow: 'auto'}})}
      >
        {headerGroups.map((headerGroup) => {
          return (
            <HeaderGroupContainer
              display="flex"
              flexDirection="row"
              alignItems="center"
              {...headerGroup.getHeaderGroupProps()}
            >
              {headerGroup.headers.map((column) => {
                return (
                  <HeaderContainer style={{paddingLeft: '1rem'}} {...column.getHeaderProps()}>
                    {column.render('Header')}
                  </HeaderContainer>
                );
              })}
            </HeaderGroupContainer>
          );
        })}
        <BodyContainer backgroundColor="white" marginBottom="medium" {...getTableBodyProps()}>
          {rows.map((row) => {
            prepareRow(row);
            return (
              <RowContainer
                {...row.getRowProps()}
                display="flex"
                flexDirection="row"
                style={{
                  backgroundColor: diffData.includes(row.original.id)
                    ? '#baeeba'
                    : Object.keys(selectedRowIds)
                        .map((rowId) => parseInt(rowId))
                        .includes(row.original.id)
                    ? '#ffc5c5'
                    : null,
                }}
              >
                {row.cells.map((cell) => {
                  return <CellWrapper {...cell.getCellProps()}>{cell.render('Cell')}</CellWrapper>;
                })}
              </RowContainer>
            );
          })}
        </BodyContainer>
      </Container>
    </Container>
  );
};
