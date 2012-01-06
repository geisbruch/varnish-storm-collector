var http = require('http'),
     sys = require('sys'),
     spawn = require('child_process').spawn,
     carrier = require('carrier'),
     ZooKeeper = require("zookeeper"),
     os = require('os');



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
      var path = req.url
      var sp = path.split('/');

      if(sp.length != 2){
          res.writeHead(403, {'Content-Type': 'application/json'});
          res.end('{ "error":"the path must be /queue"}')
          return;
      }
      var roundQueueName = sp[1] 
      res.writeHead(200, {'Content-Type': 'application/json'});
    
      if(req.method == 'DELETE' ){
          console.log("Deleting queue ["+roundQueueName+"]")
          if(queues[roundQueueName] != undefined){
              queues[roundQueueName].closeConnections()
              delete queues[roundQueueName]
          }
          res.end()
          return
      }
      
      if(queues[roundQueueName]==null){
          queues[roundQueueName]=roundQueue()
      }

      queues[roundQueueName].add(res)
      console.log("Connection taked [queue: "+roundQueueName+"]");
}).listen(1337, "0.0.0.0");

var p = 0;

commandRunner = function(){
    zkPath=process.argv[3]+"/"+os.hostname()
    console.log(zkPath)
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
   child = spawn("varnishlog",["-u"])
   carrierReader = carrier.carry(child.stdout)
   var errors = 0;
   carrierReader.on('line', function(line){
        readed++;
        for(qs in queues){
            queues[qs].send(line,null,function(error){
                errors++;
                if(errors % 100 == 0){
                    console.log("Message losts ["+errors+"]")
                }
            })
        }
   });

   process.on('SIGINT',function(){
    console.log("Exiting")
    if(zk != undefined && zk != null)
        zk.close();

        zk = new ZooKeeper({
          connect: process.argv[2],
          timeout: 22000,
          debug_level: ZooKeeper.ZOO_LOG_LEVEL_WARN,
          host_order_deterministic: false
        })

    zk.connect(function(err){
        zk.a_get(zkPath,false,function(rc,error,stat,data){
            zk.a_delete_(zkPath, stat.version,function(rc,error){
               zk.close()  
            })
        })
    })
    setTimeout(function(){
        process.exit(0)
    },1000)
   })
}

commandRunner()


roundQueue = function(){
    var obj = {lastIndex:0,conns:[],lastId:0,sessions:{}}
    var messageParser = /(\d+) +(\w*) +(c|b|-) +(.*)/
    obj.add = function(connection){
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
        if(parse != null && parse.logSession>0){
            var lSes =  parse.logSession
            if(this.sessions[lSes] == undefined || this.sessions[lSes] == null){
                this.sessions[lSes] = {"logSession":lSes,clientLogs:[],backendLogs:[],hasBackend:false}
            }
            
            if(parse["backendSession"] != undefined){
                this.sessions[lSes].hasBackend=true;
                this.sessions[lSes].backendSession=parse["backendSession"]
            }

            var messageLog = parse["messageType"]+"\t"+parse["messageLog"]
            if(parse["connectionType"] == "b"){
                this.sessions[lSes].backendLogs.push(messageLog);
            }else{
                this.sessions[lSes].clientLogs.push(messageLog);
            }

            if(parse["messageType"]=="ReqEnd"){
                var logObj = this.sessions[lSes]
                if(logObj.hasBackend){
                    bLogObj = this.sessions[logObj.backendSession];
                    if(bLogObj != null && bLogObj != undefined) {
                        logObj.backendLogs = bLogObj.backendLogs
                        delete this.sessions[logObj.backendSession]
                    }
                }
                this.sendMessageToSocket(this.sessions[lSes],valid,fail)
                delete this.sessions[lSes];
                return;
            }else{
                if(valid != null)
                    valid()
            }
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

    obj.closeConnections = function(){
        for(i in this.conns){
            this.conns[i].close()
        }
    }

    return obj
}


console.log('Server running at http://127.0.0.1:1337/');

