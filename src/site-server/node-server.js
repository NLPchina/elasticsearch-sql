var express = require('express');
var app = express();
app.use(express.static('../_site'));

app.get('/', function (req, res) {
    res.sendFile("../_site/" + "index.html" );
})
var fs = require('fs');
var siteConfiguration = JSON.parse(fs.readFileSync('site_configuration.json', 'utf8'));
var server = app.listen(siteConfiguration.port)