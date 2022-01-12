/**
 *
 * @param {string} filename
 */
export const getFileExtension = (filename) => {
  if (!filename) return;

  const match = filename.match(/(?<ext>\.[a-zA-Z0-9]{1,10})$/);
  if (!match) return;

  return match.groups.ext;
}

export const parseRange = (range) => {
  if (!range) return;

  const match = range.match(/^bytes=(?<start>\d+)-(?<end>\d*)$/);
  if (!match) return;

  return {
    start: parseInt(match.groups.start, 10),
    end: match.groups.end ? parseInt(match.groups.end, 10) : ''
  }
}
