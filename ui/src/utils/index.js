import _ from 'lodash';

export const getCurrentTheme = (theme, mode) => {
  return _.merge({}, theme, {
    colors: _.get(theme.colors.modes, mode, theme.colors),
  });
};
