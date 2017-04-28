'use strict'

var app = require('express')();
var server = require('http').Server(app);
var io = require('socket.io')(server);
var bodyParser = require('body-parser')

app.use(bodyParser.urlencoded({ extended: true }))

server.listen(3000);

app.get('/ping',function(req,res){
    io.emit('ping',{message:'hello'});
    res.send({status:1,message:'ping sent'});
});
app.post('/v1/channel/add',function(req,res){
    console.log(req.body);
    var data = {
        channel:req.body.channel,
        keywords:req.body.keyword
    };
    io.emit('channel:add',data);
    res.send({status:1,data:data});
});
app.get('/v1/collector/stop',function(req,res){
    io.emit('collector:stop',{});
    res.send({status:1});
});
app.get('/v1/collector/start',function(req,res){
    io.emit('collector:start',{});
    res.send({status:1});
});
app.get('/', function (req, res) {
    res.send({status:'ready'});
});



io.on('connection', function (socket) {
  socket.on("broadcast",function(msg){
      console.log('broadcast:',msg.name);
      msg.post.summary = msg.summary;
      socket.broadcast.emit(msg.name,msg.post);
  });
  /*socket.emit('news', { hello: 'world' });
  socket.on('my other event', function (data) {
    console.log(data);
  });*/
});