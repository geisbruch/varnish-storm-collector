var http = require('http'),
     sys = require('sys'),
     spawn = require('child_process').spawn,
     carrier = require('carrier'),
     ZooKeeper = require("zookeeper"),
     os = require('os');


var sessions={}
     
var child;

var conns = []
var i = 0;
var CRLF = '\r\n';
var queues = {}

var readed = 0;
var sended = 0;

/*
 * Create server where the server will be connected to receive the 
 * output log stream
 *
*/

http.createServer(function (req, res) {
    res.writeHead(200,"OK");
    res.end("Hola mundo");
}).listen(8080,"0.0.0.0")

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

      queues[roundQueueName].add(res.connection)
      res.writeHead(200, {'Content-Type': 'application/json'});
      console.log("Connection taked [queue: "+roundQueueName+"]");
}).listen(1337, "0.0.0.0");

var p = 0;

commandRunner = function(){
    zkPath=process.argv[3]+"/"+os.hostname()
    zk = new ZooKeeper({
          connect: process.argv[2],
          timeout: 22000,
          debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN,
          host_order_deterministic: false
    })
   console.log("Connecting to: "+process.argv[2])
   zk.connect(function(err){
     console.log(os.hostname())
     zk.a_create(zkPath,null,0,function(rc,error,path){
         zk.close()
     })   

   })
   child = spawn("varnishlog",["-u","-n","/tmp/varnish2"]);
   carrierReader = carrier.carry(child.stdout)
   var errors = 0;
   carrierReader.on('line', function(line){
        readed++;
        for(qs in queues){
            queues[qs].send(line,null,function(error){
                errors++;
                if(errors % 1000 == 0){
                    console.log("Message losts ["+errors+"]")
                }
            })
        }
   });

   process.on('SIGINT',function(){
       zk.a_delete_(zkPath,function(rc,error){
           zk.close();
       })
       console.log("Exiting")
       process.exit(0);
   })
}

commandRunner()


roundQueue = function(){
    var obj = {lastIndex:0,conns:[],lastId:0}
    var messageParser = /(\d+) +(\w*) +(c|b|-) +(.*)/
    obj.add = function(connection){
        connection.bufferSize = 100024
        connection.internalId=obj.lastId
        obj.lastId++;
        connection.on("drain",function(){
            sended++; 
            obj.conns.push(connection) 
        }) 
        this.conns.push(connection)
        connection.on("close",function(){
            console.log("Remove closed connection ["+connection.internalId+"]");
            obj.removeConnection(connection.internalId);
        })

    }
    
    obj.get = function(ses){
        if(this.size()<=0)
            return null;
        if(ses == null || ses == undefined){
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
        message = message.trim()
        var parse = this.parseLine(message)
        if(parse.logSession>0){
            var lSes =  parse.logSession
            if(sessions[lSes] == undefined || sessions[lSes] == null){
                sessions[lSes] = {"logSession":lSes,clientLogs:[],backendLogs:[],hasBackend:false}
            }
            
            if(parse["backendSession"] != undefined){
                sessions[lSes].hasBackend=true;
                sessions[lSes].backendSession=parse["backendSession"]
            }

            var messageLog = parse["messageType"]+"\t"+parse["messageLog"]
            if(parse["connectionType"] == "b"){
                sessions[lSes].backendLogs.push(messageLog);
            }else{
                sessions[lSes].clientLogs.push(messageLog);
            }

            if(parse["messageType"]=="ReqEnd"){
                var logObj = sessions[lSes]
                if(logObj.hasBackend){
                    bLogObj = sessions[logObj.backendSession];
                    if(bLogObj != null && bLogObj != undefined) {
                        logObj.backendLogs = bLogObj.backendLogs
                        delete sessions[logObj.backendSession]
                    }else{
                        console.log("Not found backend");
                    }
                }
                this.sendMessageToSocket(sessions[lSes],valid,fail)
                delete sessions[lSes];
            }else{
                if(valid != null)
                    valid()
                return;
            }
        }else{
            return;
        }
    }

    obj.sendMessageToSocket = function(message,valid,fail){
         while(this.size() > 0){
            var con = this.get()
            try{
                var mes = con.write(JSON.stringify(message))
                var crlf = con.write(CRLF);
                var res = mes && crlf;
                if(res){
                    if(valid!=null)
                        valid()
                    return;
                }else{
                     obj.removeConnection(con.internalId);                                     
                }
            }catch(e){
                obj.removeConnection(con.internalId);                                     
            }
        }
        if(fail != null)
            fail({"msg":"Error message can't be sended"})
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

    obj.removeConnection = function(id){
        if(this.size()<=0){
            return;
        }
        var idx = 0
        var found = false;
        for(i in this.conns){
            if(this.conns[i].internalId == id){
                found = true;
                idx = i;
            }

        }
        if(found)
            this.conns.splice(idx,1)
    }

    return obj
}


console.log('Server running at http://127.0.0.1:1337/');
