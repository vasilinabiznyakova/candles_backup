const fs = require('fs');
const path = require('path');

const directoryPath = './csv-files';

const files = fs.readdirSync(directoryPath);

const fileGroups = {};

files.forEach(file => {
    if (path.extname(file) === '.csv') {
        const prefix = file.split('-')[0];
        if (!fileGroups[prefix]) {
            fileGroups[prefix] = [];
        }
        fileGroups[prefix].push(file);
    }
});

Object.keys(fileGroups).forEach(prefix => {
    const outputFilePath = path.join(directoryPath, `../csv/${prefix}.csv`);
    const outputStream = fs.createWriteStream(outputFilePath);

    let headerAdded = false;

    fileGroups[prefix].forEach((file, index) => {
        const filePath = path.join(directoryPath, file);
        const fileContent = fs.readFileSync(filePath, 'utf8');

        const lines = fileContent.split('\n');

        if (!headerAdded) {
            outputStream.write(lines[0] + '\n');
            headerAdded = true;
        }

        outputStream.write(lines.slice(1).join('\n') + '\n');
    });

    outputStream.end();
    console.log(`Merged files for ${prefix} into ${outputFilePath}`);
});
