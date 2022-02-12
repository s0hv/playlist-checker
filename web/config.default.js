const config = {
  allowedExtensions: [
    '.mp4',
    '.webm',
    '.mp3',
    '.mkv',
    '.m4a',
    '.ass',

    // images
    '.webp',
    '.jpg',
    '.png'
  ],

  // file extensions for custom mimy types. Octet-stream used if no mime type is found.
  mimetypes: {},

  // Max outbound transfer per day in gigabytes
  throttleRate: 10,

  // Allow fetching mkv files as .mkv.mp4
  mkvExtensionWorkaround: true
}

export default config;
