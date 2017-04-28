"use strict";
var mysql = require('mysql');
var twitter_processor = require('./twitter_processor');
var assert = require('assert');

var pool  = mysql.createPool({
  connectionLimit : 10,
  host            : 'localhost',
  user            : 'root',
  password        : 'bronzecopper',
  database        : 'socmed_monitor'
});

var proc = new twitter_processor(pool);

describe('test processor',function(){
    
    it('can insert the tweet into table tweets',function(){
        var post = {};
        post.id = '123';
        post.channel = 'test';
        post.post_id = 'test';
        post.retweet_from_id = 'test';
        post.reply_from_id = 'test';
        post.user_id = 123;
        post.name = 'test';
        post.screen_name = 'test';
        post.location = 'test';
        post.followers_count = 'test';
        post.friends_count = 'test';
        post.statuses_count = 'test';
        post.profile_image_url = 'test';
        post.profile_image_url_https = 'test';
        post.created_at = '2017-03-01 00:00:00';
        post.timestamp_ms = (new Date().getTime());
        post.text = 'lorem ipsum';
        post.keywords = 'foo,bar';
        return proc.save(post).then(function(success){
            assert.equal(success,true);
        }).catch(function(err){
            console.log(err.message);
            assert.equal(err,null);
        });
    });
});
