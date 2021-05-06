'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

// HBase counters are stored as 8 byte binary data that the HBase Node module
// interprets as an 8 character string. Use the Javascript Buffer library to
// convert into a number
function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}
function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = (item['$'])
	});
	return stats;
}

/*
hclient.table('weather_delays_by_route_v2').row('ORDAUS').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})
 */


// testers
/*
hclient.table('shabtai_players_and_ratings_hbase').row('1503014_2_2016').get((error, value) => {
	console.info(rowToMap(value))
	console.info(value)
})

hclient.table('shabtai_players_and_ratings_hbase').scan({
		filter: {type : "PrefixFilter",
			value: "1503014"},
		maxVersions: 1},
	(err, value) => {
		console.info(value)
	})

 */

// search for player for a specific month and year
app.use(express.static('public'));
app.get('/chess-results.html',function (req, res) {
	const ID_month_year=req.query['ID_month_year'];
	hclient.table('shabtai_players_and_ratings_hbase').row(ID_month_year).get(function (err, cells) {
		if (cells != null) {
			const weatherInfo = rowToMap(cells);
			var template = filesystem.readFileSync("chess-results.mustache").toString();
			var html = mustache.render(template,  {
				ID_month_year : req.query['ID_month_year'],
				year : weatherInfo["stats:year"],
				month : weatherInfo["stats:month"],
				last_name : weatherInfo["stats:last_name"],
				first_name : weatherInfo["stats:first_name"],
				rating_standard : weatherInfo["stats:rating_standard"],
				rating_rapid : weatherInfo["stats:rating_rapid"],
				rating_blitz : weatherInfo["stats:rating_blitz"],
				federation : weatherInfo["stats:federation"],
				gender : weatherInfo["stats:gender"],
				title : weatherInfo["stats:title"],
				yob : weatherInfo["stats:yob"]
			});
			res.send(html);
		}
		//error catcher in case no results are found
		else {
			var template = filesystem.readFileSync("chess-results.mustache").toString();
			var html = mustache.render(template,  {
				ID_month_year : ['no results'],
				year : ['no results'],
				month : ['no results'],
				last_name : ['no results'],
				first_name : ['no results'],
				rating_standard : ['no results'],
				rating_rapid : ['no results'],
				rating_blitz : ['no results'],
				federation : ['no results'],
				gender : ['no results'],
				title : ['no results'],
				yob : ['no results']
			});
			res.send(html);
		}
	});
});

// trying to use multiple hbase tables
app.get('/chess-results2.html',function (req, res) {
	const ID=req.query['ID'];
	console.log(ID)
	// nesting two hclient calls
	hclient.table('latest_chess_results').row(ID).get(function (err, cells) {
		if (cells != null) {
			const weatherInfo = rowToMap(cells);
			hclient.table('shabtai_id_count').row(ID).get(function (err, cells2) {
				// no count for player case
				if (cells2 == null) {
					var template = filesystem.readFileSync("chess-results2.mustache").toString();
					var html = mustache.render(template, {
						ID: req.query['ID'],
						last_name: weatherInfo["stats:last_name"],
						first_name: weatherInfo["stats:first_name"],
						rating_standard: weatherInfo["stats:rating_standard"],
						rating_rapid: weatherInfo["stats:rating_rapid"],
						rating_blitz: weatherInfo["stats:rating_blitz"],
						games_count: 'no games'
					});
					res.send(html);
				}
				// all info available
				if (cells2 != null) {
					const weatherInfo2 = rowToMap(cells2);
					var template = filesystem.readFileSync("chess-results2.mustache").toString();
					var html = mustache.render(template, {
						ID: req.query['ID'],
						last_name: weatherInfo["stats:last_name"],
						first_name: weatherInfo["stats:first_name"],
						rating_standard: weatherInfo["stats:rating_standard"],
						rating_rapid: weatherInfo["stats:rating_rapid"],
						rating_blitz: weatherInfo["stats:rating_blitz"],
						games_count: counterToNumber(weatherInfo2["stats:games_count"])
					});
					res.send(html);
				}});
		}
		// error catcher in case a player does not exist
		else {
			var template = filesystem.readFileSync("chess-results2.mustache").toString();
			var html = mustache.render(template, {
				ID: req.query['ID'],
				last_name: 'no results',
				first_name: 'no results',
				rating_standard: 'no results',
				rating_rapid: 'no results',
				rating_blitz: 'no results',
				games_count: 'no results'
			});
			res.send(html);
		}
	});
});


// multiple years and months per player
function removePrefix(text, prefix) {
	if(text.indexOf(prefix) != 0) {
		throw "missing prefix"
	}
	return text.substr(prefix.length)
}


// for no aggregation in any way (all years and all months for each player)
// this is a helper function to get a list of the ratings in order to later graph them
// this helper function is similar between sortOriginalList, sortOriginalList2, and sortOriginalList3
// but differ slightly depending on the use case (months included, only years, and geared towards federations)
function sortOriginalList(original_list) {
	var new_list = [];
	var keys = [];
	var total_list = [];
	var blitz_list = "[";
	var standard_list = "[";
	var rapid_list = "[";
	for(var i=0;i<original_list.length;i++) {
		var current_row = original_list[i]
		var m = current_row['month']
		if (m.length == 1) {
			m = '0' + m;
		}
		keys.push(current_row['year'] + '_' + m + '_' + current_row['last_name']
			+ '_' + current_row['first_name'] + '_' + current_row['rating_standard'] + '_' + current_row['rating_blitz']
			+ '_' + current_row['rating_rapid'])
	}
	keys = keys.sort()
	for(var i=0;i<keys.length;i++){
		var current_row = keys[i].split('_')
		var insert_element = {'year': current_row[0], 'month': current_row[1], 'last_name': current_row[2],
			'first_name': current_row[3], 'rating_standard': current_row[4], 'rating_blitz': current_row[5],
			'rating_rapid': current_row[6]};

		// prepare list objects to be passed to the mustache file to produce the graphs
		var standard_elem = "[new Date('" +  current_row[1] + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[4] + ")],"
		var blitz_elem = "[new Date('" +  current_row[1] + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[5] + ")],"
		var rapid_elem = "[new Date('" +  current_row[1] + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[6] + ")],"


		blitz_list += blitz_elem;
		standard_list += standard_elem;
		rapid_list += rapid_elem;

		new_list.push(insert_element)
	}
	blitz_list += "]";
	standard_list += "]";
	rapid_list += "]";
	//blitz_list is now [ [Date, number], [blitz_elem], ...]
	total_list.push(new_list, blitz_list, standard_list, rapid_list)

	return total_list
}

// the most basic table per player
app.get('/chess-results-mult-years-months.html',function (req, res) {
	const id=req.query['ID'];
	function processYearRecord(yearRecord) {
		// error catcher
		if (yearRecord === undefined){
			console.log('no such player')
			var result = {
				year: 'no results',
				month: 'no results',
				last_name: 'no results',
				first_name: 'no results',
				rating_standard: 'no results',
				rating_blitz: 'no results',
				rating_rapid: 'no results'};
			return result}
		var result = {
			year: yearRecord['year'],
			month: yearRecord['month'],
			last_name: yearRecord['last_name'],
			first_name: yearRecord['first_name'],
			rating_standard: yearRecord['rating_standard'],
			rating_blitz: yearRecord['rating_blitz'],
			rating_rapid: yearRecord['rating_rapid']};
		return result
	}

	function airlineInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			// split the information similar to flight_and_weather examples
			var month_year = (removePrefix(cell['key'], id))
			var [x, month, year] = month_year.split('_');
			if(yearRecord === undefined)  {
				yearRecord = { year: year }
			} else if (yearRecord['year'] != year ) {
				result.push(processYearRecord(yearRecord))
				yearRecord = { year: year }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = (cell['$'])
		})
		result.push(processYearRecord(yearRecord))
		var sorted_list = sortOriginalList(result);
		return sorted_list
	}
	// nesting 3 hclient calls to capture all HBase tables in one web page
	hclient.table('shabtai_players_and_ratings_hbase').scan({
			filter: {type : "PrefixFilter",
				value: id},
			maxVersions: 1},
		(err, cells) => {
			var ai = airlineInfo(cells);
			hclient.table('shabtai_id_count').row(id).get(function (err, cells2) {
				if (cells2 != null) {
					const weatherInfo = rowToMap(cells2);
					console.log(weatherInfo)
					hclient.table('latest_chess_results').row(id).get(function (err, cells3) {
						console.log(cells3)
						if (cells3 != null) {
							const weatherInfo3 = rowToMap(cells3);
							console.log(weatherInfo3)
							var template = filesystem.readFileSync("chess-results-mult-years-months.mustache").toString();
							// here specific strings in the template change to produce the graphs dynamically
							template = template.replace('var chess_to_plot = [];',
								'var chess_to_plot ='+ai[1]+';')

							template = template.replace('var chess_to_plot2 = [];',
								'var chess_to_plot2 ='+ai[2]+';')

							template = template.replace('var chess_to_plot3 = [];',
								'var chess_to_plot3 ='+ai[3]+';')
							var html = mustache.render(template, {
								airlineInfo : ai[0],
								id : id,
								first_name : ai[0][0]['first_name'],
								last_name : ai[0][0]['last_name'],
								ID: req.query['ID'],
								rating_standard_curr: weatherInfo3["stats:rating_standard"],
								rating_rapid_curr: weatherInfo3["stats:rating_rapid"],
								rating_blitz_curr: weatherInfo3["stats:rating_blitz"],
								games_count: counterToNumber(weatherInfo["stats:games_count"])
							});
							res.send(html);
						}
						// error catcher
						else{
							var template = filesystem.readFileSync("chess-results-mult-years-months.mustache").toString();
							template = template.replace('var chess_to_plot = [];',
								'var chess_to_plot ='+ai[1]+';')

							template = template.replace('var chess_to_plot2 = [];',
								'var chess_to_plot2 ='+ai[2]+';')

							template = template.replace('var chess_to_plot3 = [];',
								'var chess_to_plot3 ='+ai[3]+';')
							var html = mustache.render(template, {
								airlineInfo : ai[0],
								id : id,
								first_name : ai[0][0]['first_name'],
								last_name : ai[0][0]['last_name'],
								ID: req.query['ID'],
								rating_standard_curr: 'no new games',
								rating_rapid_curr: 'no new games',
								rating_blitz_curr: 'no new games',
								games_count: counterToNumber(weatherInfo["stats:games_count"])
							});
							res.send(html);


						}});
				}
				// error catcher
				else {
					var template = filesystem.readFileSync("chess-results-mult-years-months.mustache").toString();
					template = template.replace('var chess_to_plot = [];',
						'var chess_to_plot ='+ai[1]+';')

					template = template.replace('var chess_to_plot2 = [];',
						'var chess_to_plot2 ='+ai[2]+';')

					template = template.replace('var chess_to_plot3 = [];',
						'var chess_to_plot3 ='+ai[3]+';')
					var html = mustache.render(template, {
						airlineInfo : ai[0],
						id : id,
						first_name : ai[0][0]['first_name'],
						last_name : ai[0][0]['last_name'],
						ID: req.query['ID'],
						rating_standard_curr: 'no new games',
						rating_rapid_curr: 'no new games',
						rating_blitz_curr: 'no new games',
						games_count: 'no games recorded'
					});
					res.send(html);
				}
			});
		})
});


// for year aggregation per player, similar to sortOriginalList2 - see comments there for more info
function sortOriginalList2(original_list) {
	var new_list = [];
	var keys = [];
	var total_list = [];
	var blitz_list = "[";
	var standard_list = "[";
	var rapid_list = "[";
	for(var i=0;i<original_list.length;i++) {
		var current_row = original_list[i]
		keys.push(current_row['year'] + '_' + current_row['last_name']
			+ '_' + current_row['first_name'] + '_' + current_row['rating_standard'] + '_' + current_row['rating_blitz']
			+ '_' + current_row['rating_rapid'])
	}
	keys = keys.sort()
	for(var i=0;i<keys.length;i++){
		var current_row = keys[i].split('_')
		var insert_element = {'year': current_row[0], 'last_name': current_row[1],
			'first_name': current_row[2], 'rating_standard': current_row[3], 'rating_blitz': current_row[4],
			'rating_rapid': current_row[5]};

		var standard_elem = "[new Date('" +  '1' + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[3] + ")],"
		var rapid_elem = "[new Date('" +  '1' + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[4] + ")],"
		var blitz_elem = "[new Date('" +  '1' + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[5] + ")],"


		blitz_list += blitz_elem;
		standard_list += standard_elem;
		rapid_list += rapid_elem;

		new_list.push(insert_element)
	}

	standard_list += "]";
	blitz_list += "]";
	rapid_list += "]";
	//blitz_list is now [ [Date, number], [blitz_elem], ...]
	total_list.push(new_list, blitz_list, standard_list, rapid_list)

	return total_list
}

app.get('/chess-results-mult-years.html',function (req, res) {
	const id=req.query['ID'];
	function processYearRecord2(yearRecord) {
		// error catcher
		if (yearRecord === undefined){
			var result = {
				year: 'no results',
				last_name: 'no results',
				first_name: 'no results',
				rating_standard: 'no results',
				rating_blitz: 'no results',
				rating_rapid: 'no results'};
			return result
		}
		var result = {
			year: yearRecord['year'],
			last_name: yearRecord['last_name'],
			first_name: yearRecord['first_name'],
			rating_standard: yearRecord['rating_standard'],
			rating_blitz: yearRecord['rating_blitz'],
			rating_rapid: yearRecord['rating_rapid']};
		return result
	}

	function airlineInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			var year = (removePrefix(cell['key'], id)) // similar parsing to examples in class
			console.log(year)
			year = year.substring(1)  // year is length 4: get rid of '_' character that's in the front
			console.log(year)
			if(yearRecord === undefined)  {
				yearRecord = { year: year }
			} else if (yearRecord['year'] != year ) {
				result.push(processYearRecord2(yearRecord))
				yearRecord = { year: year }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = (cell['$'])
		})
		console.log(processYearRecord2(yearRecord))
		result.push(processYearRecord2(yearRecord))
		var sorted_list = sortOriginalList2(result);
		return sorted_list
	}

	hclient.table('shabtai_players_and_ratings_by_year_hbase').scan({
			filter: {type : "PrefixFilter",
				value: id},
			maxVersions: 1},
		(err, cells) => {
			var ai = airlineInfo(cells);
			var template = filesystem.readFileSync("chess-results-mult-years.mustache").toString();

			template = template.replace('var chess_to_plot = [];',
				'var chess_to_plot ='+ai[1]+';')
			template = template.replace('var chess_to_plot2 = [];',
				'var chess_to_plot2 ='+ai[2]+';')
			template = template.replace('var chess_to_plot3 = [];',
				'var chess_to_plot3 ='+ai[3]+';')

			var html = mustache.render(template, {
				airlineInfo : ai[0],
				id : id,
				first_name : ai[0][0]['first_name'],
				last_name : ai[0][0]['last_name']
			});
			res.send(html)

		})
});


// for fed aggregation per year
// similar to sortOriginalList, see above for more information
function sortOriginalList3(original_list) {
	var new_list = [];
	var keys = [];
	var total_list = [];
	var blitz_list = "[";
	var standard_list = "[";
	var rapid_list = "[";
	for(var i=0;i<original_list.length;i++) {
		var current_row = original_list[i]
		keys.push(current_row['year'] + '_' + current_row['federation'] + '_' + current_row['rating_standard'] + '_' + current_row['rating_blitz']
			+ '_' + current_row['rating_rapid'])
	}
	keys = keys.sort()
	for(var i=0;i<keys.length;i++){
		var current_row = keys[i].split('_')
		var insert_element = {'year': current_row[0], 'federation': current_row[1],
			'rating_standard': current_row[2], 'rating_blitz': current_row[3],
			'rating_rapid': current_row[4]};

		var standard_elem = "[new Date('" +  '1' + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[2] + ")],"
		var rapid_elem = "[new Date('" +  '1' + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[3] + ")],"
		var blitz_elem = "[new Date('" +  '1' + '/1' + '/' + current_row[0] + "')," + "Number(" + current_row[4] + ")],"


		blitz_list += blitz_elem;
		standard_list += standard_elem;
		rapid_list += rapid_elem;

		new_list.push(insert_element)
	}
	blitz_list += "]";
	standard_list += "]";
	rapid_list += "]";
	//blitz_list is now [ [Date, number], [blitz_elem], ...]
	total_list.push(new_list, blitz_list, standard_list, rapid_list)

	return total_list
}

// gather results for federation case
app.get('/chess-results-mult-fed-years.html',function (req, res) {
	const id=req.query['ID'];
	function processYearRecord3(yearRecord) {
		// error catcher
		if (yearRecord === undefined){
			var result = {
				year: 'no results',
				federation: 'no results',
				rating_standard: 'no results',
				rating_blitz: 'no results',
				rating_rapid: 'no results'};
			return result
		}
		var result = {
			year: yearRecord['year'],
			federation: yearRecord['federation'],
			rating_standard: yearRecord['rating_standard'],
			rating_blitz: yearRecord['rating_blitz'],
			rating_rapid: yearRecord['rating_rapid']};
		return result
	}

	function airlineInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			var year = (removePrefix(cell['key'], id))
			year = year.substring(1)  // year is length 4 and get rid of '_' character
			if(yearRecord === undefined)  {
				yearRecord = { year: year }
			} else if (yearRecord['year'] != year ) {
				result.push(processYearRecord3(yearRecord))
				yearRecord = { year: year }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = (cell['$'])
		})
		result.push(processYearRecord3(yearRecord))
		var sorted_list = sortOriginalList3(result);
		return sorted_list
	}

	hclient.table('shabtai_fed_and_ratings_by_year_hbase').scan({
			filter: {type : "PrefixFilter",
				value: id},
			maxVersions: 1},
		(err, cells) => {
			var ai = airlineInfo(cells);
			var template = filesystem.readFileSync("chess-results-mult-fed-years.mustache").toString();
			template = template.replace('var chess_to_plot = [];',
				'var chess_to_plot ='+ai[1]+';')
			template = template.replace('var chess_to_plot2 = [];',
				'var chess_to_plot2 ='+ai[2]+';')
			template = template.replace('var chess_to_plot3 = [];',
				'var chess_to_plot3 ='+ai[3]+';')
			var html = mustache.render(template, {
				airlineInfo : ai[0],
				id : id
			});
			res.send(html)

		})
});




//players match to ID's
app.get('/chess-id-name.html',function (req, res) {
	const name=req.query['name'];
	function processYearRecord4(yearRecord) {
		if (yearRecord === undefined){
			var result = {
				fide_id: 'no results',
				last_name: 'no results',
				first_name: 'no results'};
			return result
		}
		var result = {
			fide_id: yearRecord['fide_id'],
			last_name: yearRecord['last_name'],
			first_name: yearRecord['first_name']};
		return result
	}

	function airlineInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			var last_name_fide_id = (removePrefix(cell['key'], name))
			var [x, last_name, fide_id] = last_name_fide_id.split('_');
			if(yearRecord === undefined)  {
				yearRecord = { last_name: last_name }
			} else if (yearRecord['last_name'] != last_name ) {
				result.push(processYearRecord4(yearRecord))
				yearRecord = { last_name: last_name }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = (cell['$'])
		})
		result.push(processYearRecord4(yearRecord))
		return result
	}

	hclient.table('shabtai_id_name_hbase').scan({
			filter: {type : "PrefixFilter",
				value: name},
			maxVersions: 1},
		(err, cells) => {
			var ai = airlineInfo(cells);
			var template = filesystem.readFileSync("chess-id-name.mustache").toString();
			var html = mustache.render(template, {
				airlineInfo: ai,
				first_name: name
			});
			res.send(html)
		});
});


// the most basic table per player's latest results
app.get('/chess-latest.html',function (req, res) {
	const id=req.query['ID'];
	function processYearRecord(yearRecord) {
		if (yearRecord === undefined){
			console.log('no such player')
			var result = {
				last_name: 'no results',
				first_name: 'no results',
				rating_standard: 'no results',
				rating_blitz: 'no results',
				rating_rapid: 'no results'};
			return result}
		var result = {
			last_name: yearRecord['last_name'],
			first_name: yearRecord['first_name'],
			rating_standard: yearRecord['rating_standard'],
			rating_blitz: yearRecord['rating_blitz'],
			rating_rapid: yearRecord['rating_rapid']};
		return result
	}

	function airlineInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			var year = cell['key']
			if(yearRecord === undefined)  {
				yearRecord = { year: year }
			} else if (yearRecord['year'] != year) {
				result.push(processYearRecord(yearRecord))
				yearRecord = { year: year }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = (cell['$'])
		})
		result.push(processYearRecord(yearRecord))
		return result
	}

	hclient.table('latest_chess_results').scan({
			filter: {type : "PrefixFilter",
				value: id},
			maxVersions: 1},
		(err, cells) => {
			var ai = airlineInfo(cells);
			var template = filesystem.readFileSync("latest_chess_results.mustache").toString();

			console.log(ai)

			var html = mustache.render(template, {
				id : id,
				first_name : ai[0]['first_name'],
				last_name : ai[0]['last_name'],
				rating_standard : ai[0]['rating_standard'],
				rating_blitz : ai[0]['rating_blitz'],
				rating_rapid : ai[0]['rating_rapid']
			});
			res.send(html)

		})
});


// the most basic table per player's count
app.get('/chess-count.html',function (req, res) {
	const id=req.query['ID'];
	function processYearRecord(yearRecord) {
		if (yearRecord === undefined){
			console.log('no such player')
			var result = {
				games_count: 'no results'};
			return result}
		var result = {
			games_count: yearRecord['games_count']};
		return result
	}

	function airlineInfo(cells) {
		var result = [];
		var yearRecord;
		cells.forEach(function(cell) {
			var year = cell['key']
			if(yearRecord === undefined)  {
				yearRecord = { year: year }
			} else if (yearRecord['year'] != year) {
				result.push(processYearRecord(yearRecord))
				yearRecord = { year: year }
			}
			yearRecord[removePrefix(cell['column'],'stats:')] = (cell['$'])
		})
		result.push(processYearRecord(yearRecord))
		return result
	}

	hclient.table('shabtai_id_count').scan({
			filter: {type : "PrefixFilter",
				value: id},
			maxVersions: 1},
		(err, cells) => {
			var ai = airlineInfo(cells);
			var template = filesystem.readFileSync("chess-count.mustache").toString();

			console.log(ai)

			var html = mustache.render(template, {
				id : id,
				games_count : counterToNumber(ai[0]['games_count'])
			});
			res.send(html)

		})
});



// Speed layer work
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);


app.get('/chess-posted.html',function (req, res) {
	var id_val = (req.query['fide_id']);
	var first_name_val = (req.query['first_name']);
	var last_name_val = (req.query['last_name']);
	var rating_standard_val = (req.query['rating_standard']);
	var rating_blitz_val = (req.query['rating_blitz']);
	var rating_rapid_val = (req.query['rating_rapid']);
	var report = {
		fide_id : id_val,
		first_name : first_name_val,
		last_name : last_name_val,
		rating_standard : rating_standard_val,
		rating_blitz : rating_blitz_val,
		rating_rapid : rating_rapid_val
	};

	console.log(report)

	kafkaProducer.send([{ topic: 'shabtai_final_test', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log("Kafka Error: " + err)
			console.log(data);
			console.log(report);
			res.redirect('submit-chess.html');
		});
});

app.listen(port);