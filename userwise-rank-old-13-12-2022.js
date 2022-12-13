const util = require('util');
const mysql = require('mysql');
var elasticSearch = require('elasticsearch');
var client = new elasticSearch.Client({
    host: 'https://vpc-stepapp-prod-bjrhqfc5fbmypxexb2kmm36dre.ap-south-1.es.amazonaws.com'
});

exports.handler = async (event) => {
    let school_id = event.school_id;
    let grade_name = event.grade_name;
    
    if ((!school_id) || (school_id == '')) {
        return { status: 404, message: '{"STATUS_CODE":0,"MESSAGE":"Invalid parameter","RESPONSE":[]}' };
    }
    
    let condition = '';
    if({grade_name} && (grade_name != '')){
        condition = " AND grade_name = '"+grade_name+"' ";
    }
    
    var con = await mysqlConnect();
    con.connect(async function(err) {
        if(err){
            if (err.code === "PROTOCOL_CONNECTION_LOST") {
                console.log("Connection was dropped, reconnecting!");
                con.destroy();
                con = await mysqlConnect();
                con.connect(async function(err) {
                    if (err) throw err;
                });
            }else {
                if (err) throw err;
            }
        }
        console.log("Mysql connected!");
    });
    
    let userRank = await getUserwiseRank(con,school_id,condition);
    let rankArray = [];
    for(const v of userRank){
        let object = {};
        object['rank'] = v['rank'];
        object['user_id'] = v['user_id'];
        object['total_score'] = v['total_score'];
        let userDetailsQuery = { "query": { "bool": { "must": [ {"match": { "asset_type": "1" }},{ "match": { "id": v['user_id'] } } ] } },"size": 1 };
        let userDetails = await getUserDetails(JSON.stringify(userDetailsQuery));
        object['fullname'] = (userDetails['fullname']) ? userDetails['fullname'] : object['user_id'];
        rankArray.push(object);
    }
    
    con.destroy();
    const response = {
        statusCode: 200,
        userRank: rankArray
    };
    return response;
};

async function getUserDetails(query){
    try {
        return new Promise((resolve, reject) => {
            client.search({
                index: "stepapp",
                type: "lrs",
                body: query
            }).then(function(resp) {
                if(resp['hits']['hits'][0] != ''){
                    var response = resp['hits']['hits'][0];
                    let final = {};
                    final['user_id'] = response['_source']['id'];
                    let first_name = response['_source']['first_name'];
                    let last_name = response['_source']['last_name'];
                    final['board_id'] = response['_source']['board_id'];
                    final['grade_id'] = response['_source']['grade_id'];
                    if((first_name == null) || (last_name == null)){
                        var fullname = response['_source']['user_id'];
                    }else{
                        fullname = first_name +' '+last_name;
                    }
                    
                    final['fullname'] = fullname;
                    final['grade_roman'] = response['_source']['grade_roman'];
                    final['grade_name'] = response['_source']['grade_name'][0];
                    final['school_name'] = response['_source']['school_name'];
                    let isBoardNameArr = Array.isArray(response['_source']['board_name']);
                    let boardName = isBoardNameArr ? response['_source']['board_name'][0] : response['_source']['board_name'] ; 
                    final['board_name'] = boardName;
                    
                    resolve(final);
                }else{
                    resolve([]);
                }
            });
        });
    }catch (error) {
        console.log(error);
    }
}

async function getUserwiseRank(con,school_id,condition){
    let query = "SELECT user_id, total_score FROM user_score WHERE school_id in ("+school_id+") "+condition+" order by total_score desc limit 10";
    con.query = util.promisify(con.query);
    let userResult = await con.query(query);
    var leaderboard = [];
    let i = 1;
    if(userResult.length > 0) {
        let jsonData = JSON.stringify(userResult);
        let parseData = JSON.parse(jsonData);
        parseData.forEach(async function(v,k){
            leaderboard[k] = {};
            leaderboard[k]['rank'] = i++;
            leaderboard[k]['user_id'] = v['user_id'];
            leaderboard[k]['total_score'] = v['total_score'];
    
        });
    }
    return leaderboard;
}

async function mysqlConnect(){
    var con = mysql.createConnection({
        host: process.env.host,
        user: process.env.user,
        password: process.env.password,
        database: process.env.database,
        insecureAuth : process.env.insecureAuth,
    });
    return con;
}