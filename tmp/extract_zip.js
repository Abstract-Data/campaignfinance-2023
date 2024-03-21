// Write a script to extract zip file /tmp/texas.zip to /tmp/texas
// Usage: node extract_zip.js <path>
// Example: node extract_zip.js tmp/zip.zip
// Output: tmp/zip

const fs = require('fs');
const path = require('path');
const unzipper = require('unzipper');

const args = process.argv.slice(2);
const zipPath = args[0];

if (!zipPath) {
    console.log('Please provide zip file path');
    process.exit(1);
    }

const zipFileName = path.basename(zipPath);
const zipFileDir = path.dirname(zipPath);
const zipFileDirName = path.basename(zipFileDir);
const zipFileDirPath = path.dirname(zipFileDir);

const extractPath = path.join(zipFileDirPath, zipFileDirName);

