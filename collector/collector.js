var http = require('http'),
     sys = require('sys'),
     spawn = require('child_process').spawn,
     carrier = require('carrier'),
     ZooKeeper = require("zookeeper"),
     os = require('os');
var child;

var conns = []
var i = 0;
var CRLF = '\r\n';
var queues = {}

var readed = 0;
var sended = 0;

http.createServer(function (req, res) {
      res.writeHead(200, {'Content-Type': 'application/json'});
      res.end('{}')
}).listen(8080, "127.0.0.1");


/*
 * Create server where the server will be connected to receive the 
 * output log stream
 *
*/
http.createServer(function (req, res) {
      var path = req.url
      var sp = path.split('/');

      if(sp.length != 2){
          res.writeHead(403, {'Content-Type': 'application/json'});
          res.end('{ "error":"the path must be /queue"}')
          return;
      }
      var roundQueueName = sp[1] 
      
      if(queues[roundQueueName]==null){
          queues[roundQueueName]=roundQueue()
      }

      queues[roundQueueName].add(res)
      res.writeHead(200, {'Content-Type': 'application/json'});
      console.log("Connection taked [queue: "+roundQueueName+"]");
}).listen(1337, "127.0.0.1");

var p = 0;

commandRunner = function(){
/*    zk = new ZooKeeper({
          connect: process.argv[2],
          timeout: 200000,
          debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN,
          host_order_deterministic: false
    })
    console.log("Connecting to: "+process.argv[2])
    zk.connect(function(err){})
//    zk.a_create('/1/2/3',{"hola":"mundo"},0,function(rc,error,path){})   
    zk.a_get('/1/2/3',true,function(rc,error,stat,data){
    console.log("Error %j, %j, %j, %s",rc,error,stat,JSON.parse(data).data);
    })

    zk.a_set('/1/2/3',JSON.stringify({"data":"dadas","pepep":"dasd"}),-1,function(rc,error,stat){
    console.log("Error %j, %j, %j",rc,error,stat);
    })
 */
   child = spawn("varnishlog",["-u"]);
    carrierReader = carrier.carry(child.stdout)
    carrierReader.on('line', function(line){
        readed++;
        for(qs in queues){
            queues[qs].send(line,null,function(error){
                console.log("Error: "+error.msg)
            })
        }
    });
}

commandRunner()


roundQueue = function(){
    var obj = {lastIndex:0,conns:[]}
    var messageParser = /(\d+) +(\w*) +(c|b|-) +(.*)/
    obj.add = function(connection){
        connection.on("drain",function(){
            console.log("Connection restablished "+connection)
            obj.conns.push(connection)
        }) 
        this.conns.push(connection)
    }
    
    obj.get = function(ses){
        if(this.size()<=0)
            return null;
        if(ses!=null){
            this.lastIndex++;
            if(this.lastIndex >= this.conns.length)
                this.lastIndex=0;
    
            var ret = this.conns[this.lastIndex]
        
            return ret
        }else{
            var mod = ses % this.size();
            return this.conns[mod];
        }
    }

    obj.size = function(){
        return this.conns.length
    }

    obj.send = function(message,valid,fail){
        var parse = this.parseLine(message)
        var sessions = null
        if(parse.logSession>0){
            sessions = []
            if(parse["backendSession"] != undefined){
                sessions.push(parse["backendSession"]);
            }
            sessions.push(parse.logSession);
        }else{
            return;
        }

        errors = false;

        for(i in sessions){
            var ses = sessions[i]
            var sended = false;
            while(this.size()>0 && !sended){
                var con = this.get(ses)
                console.log
                var mes = con.write(ses+"\t"+message)
                var crlf = con.write(CRLF);
                var res = mes && crlf;
                if(res){
                    if(valid!=null)
                        valid();
                    sended++;
                    sended = true;
                }else{
                    console.log("Connection removed ["+this.lastIndex+"]")
                    this.removeActualConnection();
                }
            }
            if(!sended && fail!=null){
                 fail({msg:"No conns available"})
                 return;
            }

        }
    }

    obj.parseLine = function(line){
        var parse = messageParser.exec(line)
        if(!parse){
            return null;
        }
        var ret = {"logSession":parse[1],"messageType":parse[2],"connectionType":parse[3],"messageLog":parse[4]}
        if(ret.messageType=="Backend"){
            var idx = ret.messageLog.indexOf(' ');
            ret["backendSession"]=ret.messageLog.substring(0,idx)
        }

        return ret
    }

    obj.removeActualConnection = function(){
        if(this.size()<=0){
            return;
        }
        this.conns.splice(this.lastIndex,1)
    }

    return obj
}


console.log('Server running at http://127.0.0.1:1337/');
