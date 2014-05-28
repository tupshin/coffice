var express = require('express');
var router = express.Router();
var dateFormat = require('dateformat');

var cql = require('node-cassandra-cql');
var client = new cql.Client({hosts: ['208.96.49.194', '208.96.49.195', '208.96.49.196'], keyspace: 'coffice'});


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

router.param('type', function(req, res, next, type) {
	// validation step
	req.type = type;
	next();	
});

///Actual routes

router.post('/sensor', function(req,res){
	
	var sensorDate = new Date(req.body.value_ts);
	var sensorType = req.body.sensor_id.substring( 0, req.body.sensor_id.indexOf("_"));
	var quarterHour;
	var x = parseInt(dateFormat(sensorDate, "MM"));

	
	switch(true) {
		
	case (x <= 15):
		quarterHour = '1';
		break;
	case (x <= 30):
		quarterHour = '2';
		break;
	case (x <= 45):
		quarterHour = '3';
		break;
	case (x <= 60):
		quarterHour = '4';
		break;
	}
	
	var queries = [
		{	
		  query: "insert into sensor_last (sensor_type, sensor_id, value, value_ts) values('" + 
		 sensorType + 
		  "', '" + req.body.sensor_id + "', " + req.body.value + ", '" + dateFormat(sensorDate, "yyyymmddHHMMss") + "')"	 
	   },
		{	
	  	  query: "insert into sensor_date (sensor_id, value_date, value_ts, value) values('" + req.body.sensor_id + 
	  	"', '" + dateFormat(sensorDate, "yyyymmdd") + "', '" + dateFormat(sensorDate, "yyyymmddHHMMss") + "'," + req.body.value + ")" 
   		}
		];
		
		var query3  = "update event_count_date set event_count = event_count + 1 where sensor_type = '" + 
		  	  sensorType + "' and event_date = '" + 
		  		dateFormat(sensorDate, "yyyymmdd") + "' and event_ts = '" + dateFormat(sensorDate, "yyyymmddHHMMss") + "'" 

		var query4  = "update event_count_date set event_count = event_count + 1 where sensor_type = 'all' and event_date = '" + 
	  			dateFormat(sensorDate, "yyyymmdd") + "' and event_ts = '" + dateFormat(sensorDate, "yyyymmddHHMMss") + "'" 
		
		
			
		var query5  = "update sensor_hour_avg set hour_count = hour_count + 1, hour_total = hour_total + " + parseInt(req.body.value * 100) +
					" where sensor_id = '" + req.body.sensor_id + "' and event_date_hour = '" + dateFormat(sensorDate, "yyyymmddHH") + "'"
			
		var query6  = "update sensor_type_hour_avg set hour_count = hour_count + 1, hour_total = hour_total + " + parseInt(req.body.value * 100) +
					" where sensor_id = '" + req.body.sensor_id + "' and event_date_hour = '" + 
					dateFormat(sensorDate, "yyyymmddHH") + "' and sensor_type = '" + sensorType + "'"
		
		var query7  = "update sensor_qtr_hour_avg set qtr_hour_count = qtr_hour_count + 1, qtr_hour_total = qtr_hour_total + " + parseInt(req.body.value * 100) +
					" where sensor_id = '" + req.body.sensor_id + "' and event_date_qtr_hour = '" + dateFormat(sensorDate, "yyyymmddHH") + quarterHour + "'"
		
		//var query5  = "update sensor_hour_avg set hour_count = hour_count + 1, hour_total = hour_total + ? where sensor_id = ? and event_date_hour = ?"

		//var params5 = [req.body.value, req.body.sensor_id, dateFormat(sensorDate, "yyyymmddHH")]; 
		
	var consistency = cql.types.consistencies.quorum;
	client.executeBatch(queries, consistency, function(err) {
	  if (err) {
		  console.log(err);
		  res.send(err);
	  }
	  else {
	  }
	});
  	client.execute(query3, consistency, function(err) {
	 if (err) {
		 console.log(err);
  		  res.send(err);
  	  }
  	});
  	client.execute(query4, consistency, function(err) {
	 if (err) {
		 console.log(err);
  		  res.send("4: " + err);
  	  }
  	});
  	client.execute(query5, consistency, function(err) {
	 if (err) {
		 console.log("5: " + err);
  		  res.send(err);
  	  }
  	});
  	client.execute(query6, consistency, function(err) {
	 if (err) {
		 console.log("6: " + err);
  		  res.send(err);
  	  }
  	});
  	client.execute(query7, consistency, function(err) {
	 if (err) {
		 console.log("7: " + err);
  		  res.send(err);
  	  }
  	});
	res.send(200);
})


router.get('/count/type/:name', function(req, res) {
	var now = new Date();
	var now_utc = new Date(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), 
	                  now.getUTCHours(), now.getUTCMinutes(), now.getUTCSeconds());
	var rows = [];
	var query = "select event_ts, event_count from event_count_date where sensor_type = '" + req.name + "' and event_date = '" + 
				dateFormat( now_utc, "yyyymmdd") + "' limit 1"
	
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
		rows.push({"timestamp": row.event_ts, "value": parseInt(row.event_count)});
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
	var rows = [];
	var query = "select event_ts, event_count from event_count_date where sensor_type = '" + req.name + "' and event_date = '" + 
				dateFormat( now_utc, "yyyymmdd") + "' limit " + req.qty
	
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
//		rows["ts:" + row.event_ts] = parseInt(row.event_count);
		rows.push({"timestamp": row.event_ts, "value": parseInt(row.event_count)});
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.jsonp(rows);
	  }
	);

});

router.get('/sensor_hour_avg/sensor_id/:name/qty/:qty', function(req, res) {
	var rows = [];
	var query = "select * from sensor_hour_avg where sensor_id = '" + req.name + "' limit " + req.qty
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
		var hour_avg = (row.hour_total / row.hour_count) / 100;
		rows.push({"sensor_id": row.sensor_id, "event_date_hour": row.event_date_hour, "hour_avg": hour_avg});
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.jsonp(rows);
	  }
	);
});

router.get('/sensor_qtr_hour_avg/sensor_id/:name/qty/:qty', function(req, res) {
	var rows = [];
	var query = "select * from sensor_qtr_hour_avg where sensor_id = '" + req.name + "' limit " + req.qty
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
		var qtr_hour_avg = (row.qtr_hour_total / row.qtr_hour_count) / 100;
		rows.push({"sensor_id": row.sensor_id, "event_date_qtr_hour": row.event_date_qtr_hour, "qtr_hour_avg": qtr_hour_avg});
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.jsonp(rows);
	  }
	);
});

//Return hour averages for all sensor in a given type (limit)
router.get('/sensor_hour_avg/sensor_type/:type/qty/:qty', function(req, res) {
	var rows = [];
	var query = "select * from sensor_type_hour_avg where sensor_type = '" + req.type + "' limit " + req.qty
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
		var hour_avg = (row.hour_total / row.hour_count) / 100;
		rows.push({"sensor_id": row.sensor_id, "event_date_hour": row.event_date_hour, "hour_avg": hour_avg});
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.jsonp(rows);
	  }
	);
});


router.get('/sensor/:sensor_id/day/:day', function(req, res) {
	var rows = [];
	var query = "select * from sensor_date where sensor_id = '" + req.sensor_id + "' and value_date = '" + req.day + "'"
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
//		rows["ts:" + row.event_ts] = parseInt(row.event_count);
		rows.push({"sensor_id": row.sensor_id, "sensor_date": row.sensor_date, "value_ts": row.value_ts, "value": parseFloat(row.value)});
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.jsonp(rows);
	  }
	);

});

router.get('/sensor/:sensor_id/day/:day/qty/:qty', function(req, res) {
	var rows = [];
	var query = "select * from sensor_date where sensor_id = '" + req.sensor_id + "' and value_date = '" + req.day + "' limit " + req.qty
	client.eachRow(query,function(n, row) {
	    //the callback will be invoked per each row as soon as they are received
//		rows["ts:" + row.event_ts] = parseInt(row.event_count);
		rows.push({"sensor_id": row.sensor_id, "sensor_date": row.value_date, "value_ts": row.value_ts, "value": parseFloat(row.value)});
	  },
	  function (err, rowLength) {
	    if (err) console.log(err);
		res.jsonp(rows);
	  }
	);

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