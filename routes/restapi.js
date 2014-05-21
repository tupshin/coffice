var express = require('express');
var router = express.Router();
var dateFormat = require('dateformat');

var cql = require('node-cassandra-cql');
var client = new cql.Client({hosts: ['208.96.49.194', '208.96.49.194', '208.96.49.194'], keyspace: 'coffice'});


/* GET users listing. */
router.get('/', function(req, res) {
	
	res.send('respond with a resource');
});

//Params Handling
router.param('name', function(req, res, next, name) {
	// validation step
	req.name = name;
	next();	
});

router.param('sensor_id', function(req, res, next, sensor_id) {
	// validation step
	req.sensor_id = sensor_id;
	next();	
});

router.param('day', function(req, res, next, day) {
	// validation step
	req.day = day;
	next();	
});

router.param('start', function(req, res, next, start) {
	// validation step
	req.start = start;
	next();	
});

router.param('end', function(req, res, next, end) {
	// validation step
	req.end = end;
	next();	
});

router.param('qty', function(req, res, next, qty) {
	// validation step
	req.qty = qty;
	next();	
});

///Actual routes

router.post('/sensor', function(req,res){
	
	var sensorDate = new Date(req.body.value_ts);
	
	var queries = [
		{	
		  query: "insert into sensor_last (sensor_type, sensor_id, value, value_ts) values('" + 
		 req.body.sensor_id.substring( 0, req.body.sensor_id.indexOf("_")) + 
		  "', '" + req.body.sensor_id + "', " + req.body.value + ", '" + dateFormat(sensorDate, "yyyymmddHHMMss") + "')"	 
	   },
		{	
	  	  query: "insert into sensor_date (sensor_id, value_date, value_ts, value) values('" + req.body.sensor_id + 
	  	"', '" + dateFormat(sensorDate, "yyyymmdd") + "', '" + dateFormat(sensorDate, "yyyymmddHHMMss") + "'," + req.body.value + ")" 
   		}
		];
		
	var	query1 =  "update event_count_date set event_count = event_count + 1 where sensor_type = '" + 
		  req.body.sensor_id.substring( 0, req.body.sensor_id.indexOf("_")) + "' and event_date = '" + 
		  dateFormat(sensorDate, "yyyymmdd") + "' and event_ts = '" + dateFormat(sensorDate, "yyyymmddHHMMss") + "'" 
	
	var	query2 =  "update event_count_date set event_count = event_count + 1 where sensor_type = 'all' and event_date = '" + 
	  	dateFormat(sensorDate, "yyyymmdd") + "' and event_ts = '" + dateFormat(sensorDate, "yyyymmddHHMMss") + "'" 
	  
	var consistency = cql.types.consistencies.quorum;
	client.executeBatch(queries, consistency, function(err) {
	  if (err) {
		  console.log(err);
		  res.send(err);
	  }
	  else {
	  }
	});
  	client.execute(query1, function(err) {
	 if (err) {
  		  res.send(err);
  	  }
  	});
  	client.execute(query2, function(err) {
	 if (err) {
  		  res.send(err);
  	  }
  	});
	res.send(200);
})

router.get('/sensor/types/:name', function(req, res) {
	var rows = [];
	var query = "select * from sensor_last where sensor_type = '" + req.name + "'"
	
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
		rows.push('value_ts: ' + row.value_ts + ', value: ' + parseInt(row.value));
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.json(rows);
	  }
	);
});

router.get('/count/type/:name', function(req, res) {
	var now = new Date();
	var now_utc = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 
	                  now.getUTCHours(), now.getUTCMinutes(), now.getUTCSeconds());
	var rows = {};
	var query = "select event_ts, event_count from event_count_date where sensor_type = '" + req.name + "' and event_date = '" + 
				dateFormat( now_utc, "yyyymmdd") + "' limit 1"
	
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
		rows["ts:" + row.event_ts] = parseInt(row.event_count);
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.jsonp(rows);
	  }
	);
});

router.get('/count/type/:name/qty/:qty', function(req, res) {
	var now = new Date();
	var now_utc = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 
	                  now.getUTCHours(), now.getUTCMinutes(), now.getUTCSeconds());
	var rows = {};
	var query = "select event_ts, event_count from event_count_date where sensor_type = '" + req.name + "' and event_date = '" + 
				dateFormat( now_utc, "yyyymmdd") + "' limit " + req.qty
	
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
		rows["ts:" + row.event_ts] = parseInt(row.event_count);
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.jsonp(rows);
	  }
	);

});

router.get('/sensor/type/:name', function(req, res) {
	var rows = [];
	var query = "select * from sensor_last where sensor_type = '" + req.name + "'"
	client.execute(query, function(err, result, rowLength)	{
		if (err){
			console.log('db error')
		}
		else{
			console.log('rowLength: ' + rowLength)
			//this is pointless, but may want to restructure return values later...
			for( var i = 0; i < rowLength; i++ ) {
				rows.push(result.rows[i]);
				console.log(i);
			}
			res.json(result);
		}
	});
});

router.get('/sensor/:sensor_id/day/:day', function(req, res) {
	var rows = [];
	var query = "select * from sensor_date where sensor_id = '" + req.sensor_id + "' and value_date = '" + req.day + "'"
	client.execute(query, function(err, result)	{
		if (err){
			console.log('db error')
			res.send(err);
		}
		else{
			var resultArray = [];
			resultArray.push(result);
			console.log(resultArray.length)
			//this is pointless, but may want to restructure return values later...
			for( var i = 0; i < result.length; i++ ) {
				rows.push(result.rows[i]);
				console.log(result.rows[i]);
			}
			res.json(result);
		}
	});
});

router.get('/sensor/:sensor_id/day/:day/start/:start/end/:end', function(req, res) {
	var rows = [];
	var query = "select * from sensor_date where sensor_id = '" + req.sensor_id + "' and value_date = '" + req.day +
			"' and value_ts > '" + req.start + "' and value_ts < '" + req.end + "'"
	client.execute(query, function(err, result)	{
		if (err){
			console.log('db error')
			res.send(err);
		}
		else{
			
			//this is pointless, but may want to restructure return values later...
			for( var i = 0; i < result.rowCount; i++ ) {
				rows.push(result.rows[i]);
				console.log(result.rows[i]);
			}
			res.json(result);
		}
	});
});




module.exports = router;