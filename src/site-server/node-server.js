var express = require('express');
const path = require('path');
var app = express();
app.use(express.static(path.join(__dirname, '../_site')));

app.get('/', function (req, res) {
    res.sendFile(path.join(__dirname, '../_site/index.html'));
})
var fs = require('fs');
var siteConfiguration = JSON.parse(fs.readFileSync(path.join(__dirname, './site_configuration.json'), 'utf8'));
var server = app.listen(siteConfiguration.port);