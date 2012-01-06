var zmq = require('zmq')
  , pub = zmq.socket('req')
  , router = zmq.socket('xrep')
  , dealer = zmq.socket('xreq')
  , sub2 = zmq.socket('rep')
  , sub1 = zmq.socket('rep');

var buf1 = new Buffer("1")
var buf2 = new Buffer("2")
var buf3 = new Buffer("3")

dealer.bind("tcp://127.0.0.1:1337",function(){
    console.log("Dealer binded");
})

router.bind("tcp://127.0.0.1:1338",function(){
    console.log("Router binded")
})

pub.setsockopt("identity",buf3)
pub.connect("tcp://127.0.0.1:1338");


sub1.setsockopt("identity",buf1)
sub2.setsockopt("identity",buf2)

sub1.connect("tcp://127.0.0.1:1337");
sub2.connect("tcp://127.0.0.1:1337");

sub2.on('message',function(msg){
   console.log("2: "+msg);
   sub2.send("fui el 2");
})
sub1.on('message',function(msg){
    console.log("1: "+msg);
    sub1.send("fui el 1");
})


send = function(){
    pub.send("ready")
    pub.send("data")
}

router.on('message',function(msg){
        console.log(msg.toString())
        router.send("alalal")
        console.log(router)
        console.log("respondi")
})

setInterval(send,500)


pub.on('message',function(msg){
    console.log(msg)
})

