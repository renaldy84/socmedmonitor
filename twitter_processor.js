"use strict";

class twitter_processor{

    constructor(pool,redis){
        //do nothing
        this.pool = pool;
        this.redis = redis;
    }
    count_realtime_data(post){
        return new Promise((resolve,reject)=>{
            this.update_tweets_total(this.redis, post);
            this.update_retweets_total(this.redis, post);
            this.update_reach_total(this.redis, post);
            resolve(true);
        });
    }
    update_tweets_total(redis,post){
        var key = post.channel+':t';
        if(typeof post.retweet_from_id === 'undefined' || post.retweet_from_id == ''){
            redis.get(key,function(err,total){
                if(total==null){
                    redis.set(key,1);
                }else{
                    redis.set(key,parseInt(total)+1);
                }
            });    
        }
    }
    update_retweets_total(redis,post){
        var key = post.channel+':rt';
        if(typeof post.retweet_from_id !== 'undefined'){
            redis.get(key,function(err,total){
                if(total==null){
                    redis.set(key,1);
                }else{
                    redis.set(key,parseInt(total)+1);
                }
            });    
        }
    }
    update_reach_total(redis,post){
        var key = post.channel+':reach';
       
            redis.get(key,function(err,total){
                if(total==null){
                   redis.set(key,1);
                }else{
                   redis.set(key,parseInt(total) + post.followers_count + post.friends_count);
                }
            });    
        
    }
    save(post){
        return new Promise((resolve,reject)=>{
            this.pool.getConnection((err,conn)=>{
                if(err){
                    reject(err);
                }else{
                    var db = this.pool.config.connectionConfig.database;
                    conn.query(`INSERT IGNORE INTO ${db}.tweets(
                        channel,post_id,retweet_from_id,
                        reply_from_id,user_id,name,screen_name,
                        location,followers_count,friends_count,statuses_count,
                        profile_image_url,profile_image_url_https,
                        created_at,timestamp_ms,text,keywords
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,[
                       
                        post.channel,
                        post.post_id,
                        post.retweet_from_id,
                        post.reply_from_id,
                        post.user_id,
                        post.name,
                        post.screen_name,
                        post.location,
                        post.followers_count,
                        post.friends_count,
                        post.statuses_count,
                        post.profile_image_url,
                        post.profile_image_url_https,
                        post.created_at,
                        post.timestamp_ms,
                        post.text,
                        post.keywords
                    ],function(err,rs){
                        conn.release();
                        if(err) reject(err);
                        else resolve(true);
                        
                    });
                }
            });
            
        });
    }

    
}

module.exports = twitter_processor;


