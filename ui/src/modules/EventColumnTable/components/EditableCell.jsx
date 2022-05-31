import {useState, useEffect} from 'react';
import {Container, Input} from 'components/ui';

export const EditableCell = ({
  value: initialValue,
  row: {index, original},
  column: {id, Header},
  onBlurRow,
  types,
}) => {
  const [value, setValue] = useState(initialValue);
  const [editing, setEditing] = useState(false || Header === 'name');

  useEffect(() => {
    setValue(initialValue);
  }, [initialValue]);

  const onChange = (e) => {
    setValue(e.target.value);
  };

  // TODO: validation on name to ensure it's not empty
  const onBlur = () => {
    onBlurRow(index, id, value);
    setEditing(false);
  };

  const onChangeDropdown = (e) => {
    setValue(e.target.value);
    onBlurRow(index, id, e.target.value);
    setEditing(false);
  };

  const showValueOrDefault = (value) => {
    return value === '' ? '-' : value;
  };

  // TODO: clean this up
  // if it's not a new row, just show the value
  if (!original.new) {
    return <Container>{showValueOrDefault(value)}</Container>;
  } else {
    if (Header === 'type') {
      return (
        <select onChange={onChangeDropdown} value={value}>
          {types.map((type, index) => {
            return (
              <option key={`${type}_${index}`} value={type}>
                {type}
              </option>
            );
          })}
        </select>
      );
    } else if (!editing) {
      return (
        <Container
          width="100%"
          height="100%"
          display="flex"
          alignItems="center"
          style={{cursor: 'pointer'}}
          onClick={() => setEditing(true)}
        >
          {showValueOrDefault(value)}
        </Container>
      );
    } else {
      return (
        <Input
          autoFocus
          type="text"
          value={value}
          onChange={onChange}
          onBlur={onBlur}
          placeholder="enter a value"
          paddingLeft="small"
        />
      );
    }
  }
};
