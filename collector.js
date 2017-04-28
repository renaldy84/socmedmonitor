var TwitterStreamChannels = require('twitter-stream-channels');
var twitter_processor = require('./twitter_processor');
var config = require('./config');
var mysql = require('mysql');
var io = require('socket.io-client');
var socket = io(config.socket_server.host);
var async = require('async');
var moment = require('moment');

var redis = require("redis"),
    redisClient = redis.createClient();

// if you'd like to select database 3, instead of 0 (default), call
// client.select(3, function() { /* ... */ });

redisClient.on("error", function (err) {
    console.log("Error " + err);
});

var pool  = mysql.createPool({
  connectionLimit : 10,
  host            : config.mysql.host,
  user            : config.mysql.user,
  password        : config.mysql.password,
  database        : config.mysql.database,
});

var processor = new twitter_processor(pool,redisClient);

var client = new TwitterStreamChannels(require('./credentials.json'));
 
var channels = {
    //"trump" : ['trump','#trump'],
   // "pilkada" : ['ahok','anies'],
};


var channel_loaded = false;
redisClient.get('channels',function(err,data){
    if(data!=null){
        channels = JSON.parse(data);  
        startStream();
    }
    channel_loaded = true;
});

var stream = false;



socket.on("channel:add",function(msg){
    if(channel_loaded){
        console.log(moment().format(),'channel:add',msg);
        if(typeof msg.channel !== 'undefined' && typeof msg.keywords !== 'undefined'){
            if(msg.keywords.length>0){
                channels[msg.channel] = msg.keywords;
                console.log(moment().format(),'channels',channels);
                redisClient.set('channels',JSON.stringify(channels));
                if(!stream){
                    startStream();
                }else{
                    stream.stop();
                    stream.removeAllListeners('channels');
                    startStream();
                }
            }else{
                console.log(moment().format(),'channel:add','invalid format');
            }
        }
    }else{
        console.log(moment().format(),'channel:add','channel not loaded yet');
    }
});

socket.on("collector:stop",function(msg){
    console.log(moment().format(),'collector:stop');
    if(stream){
        stream.stop();
        stream.removeAllListeners('channels');
    }
});
socket.on("collector:start",function(msg){
    console.log(moment().format(),'collector:start');
    startStream();
});

var distribute = function(post,channels){
    if(typeof post === 'undefined') return false;
    if(typeof channels === 'undefined') return false;

    var ch = [];
    
    for(var channel in channels){
        ch.push(channel);
    }
    
    
    async.eachSeries(ch,function(item,next){
        post.channel = item;
        
        //emits the post in realtime & also the realtime summary
        var summary = {
            tweets:0,
            rt:0,
            reach:0
        };
        redisClient.get(item+':t',function(err,total){
            if(total!=null){
                summary.tweets = total;
            }
            redisClient.get(item+':rt',function(err,total){
                if(total!=null){
                    summary.rt = total;
                }   
                redisClient.get(item+':reach',function(err,total){
                    if(total!=null){
                        summary.reach = total;
                    }   
                    var broadcastmsg = {post:post,name:item+':post',summary:summary};
                    console.log(broadcastmsg);
                    socket.emit('broadcast',broadcastmsg);
                });
            });
        });
       
        //console.log(item+':post');
        processor.save(post).then(function(success){
           // console.log(moment().format(),item,'> ','@'+post.screen_name,post.text);
           processor.count_realtime_data(post).then(function(success){
            next();
           }).catch(function(err){
                console.log(moment().format(),'ERROR',err.message);
                next();   
           });
            
        }).catch(function(err){
            console.log(moment().format(),'ERROR',err.message);
            next();
        });
    },
    function(err){
        if(err){console.log(moment().format(),'ERROR',err.message);}
    });
    
    

}


/*
stream.on('channels/pilkada',function(tweet){
    var post = {};
    post.post_id = tweet.id_str;
    post.channel = 'pilkada';
    post.retweet_from_id = '';
    post.reply_from_id = '';
    post.user_id = tweet.user.id;
    post.text = tweet.text;
    post.name = tweet.user.name;
    post.screen_name = tweet.user.screen_name;
    post.location = tweet.user.location;
    post.followers_count = tweet.user.followers_count;
    post.friends_count = tweet.user.friends_count;
    post.statuses_count = tweet.user.statuses_count;
    post.profile_image_url = tweet.user.profile_image_url;
    post.profile_image_url_https = tweet.user.profile_image_url_https;
    post.created_at = tweet.created_at;
    post.timestamp_ms = tweet.timestamp_ms;
    post.keywords = tweet['$keywords'].join(',');
    if(typeof tweet.retweeted_status !== 'undefined'){
        post.retweet_from_id = tweet.retweeted_status.id_str;
        //save retweeted status here

        //-->
    }
    processor.save(post).then(function(success){
        console.log('pilkada> ','@'+post.screen_name,post.text);
    }).catch(function(err){
        console.log('ERROR',err.message);
    });
});
*/

 
//If you want, you can listen on all the channels and pickup the $channels added by the module 
//It contains the channel and the keywords picked up in the tweet 
//stream.on('channels',function(tweet){ 
//    console.log(tweet.$channels,tweet.text);//any tweet with any of the keywords above 
//}); 
 
//If you're really picky, you can listen to only some keywords 
//stream.on('keywords/javascript',function(tweet){ 
//    console.log(tweet.text);//any tweet with the keyword "javascript" 
//}); 
 /*
setTimeout(function(){
    stream.stop();//closes the stream connected to Twitter 
    console.log('>stream closed after 100 seconds');
},100000);*/




var startStream = function(){
    console.log('startStream');
    stream = client.streamChannels({track:channels});
    stream.on('channels',function(tweet){
        var post = {};
        post.post_id = tweet.id_str;
        post.channel = '';
        post.retweet_from_id = '';
        post.reply_from_id = '';
        post.user_id = tweet.user.id;
        post.text = tweet.text;
        post.name = tweet.user.name;
        post.screen_name = tweet.user.screen_name;
        post.location = tweet.user.location;
        post.followers_count = tweet.user.followers_count;
        post.friends_count = tweet.user.friends_count;
        post.statuses_count = tweet.user.statuses_count;
        post.profile_image_url = tweet.user.profile_image_url;
        post.profile_image_url_https = tweet.user.profile_image_url_https;
        post.created_at = tweet.created_at;
        post.timestamp_ms = tweet.timestamp_ms;
        post.keywords = tweet['$keywords'].join(',');
        if(typeof tweet.retweeted_status !== 'undefined'){
            post.retweet_from_id = tweet.retweeted_status.id_str;
            //save retweeted status here

            //-->
        }
        distribute(post,tweet['$channels']);
    });
}
