var logger = require("../utils/logger");
var reportOpertaion = require("../db/reportOperations");
var moment = require("moment");
var async = require("async");
var getTargetCountByBranchId = require("../controllers/targetController").getTargetCountByBranchId;
var db = require("../db/config");

var _ = require("lodash");

exports.getInventriesDetail = function(req,res,next) {
    var value = {
        startDate: req.query.startDate ? getFormattedStartDate(req.query.startDate):null,
        endDate: req.query.endDate ? getFormattedEndDate(req.query.endDate):null
    }
    reportOpertaion.getAllInventries(value, function(err, result){
        if (err) {
            logger.error(err);
            return next(err);
          }
          res.json(result);
    });
}

exports.getInventriesCount = function(req,res,next) {
    var value = {
        startDate: req.query.startDate ? getFormattedStartDate(req.query.startDate):null,
        endDate: req.query.endDate ? getFormattedEndDate(req.query.endDate):null,
        branchId : req.query.branchId ? req.query.branchId : null
    }
    reportOpertaion.getAllInventriesCount(value, function(err, result){
        if (err) {
            logger.error(err);
            return next(err);
          }
          res.json(result);
    });
}


exports.getInventriesCountByBranchId =  function(req,res,next) {
    var summaryArr = [];
    var monthArray = enumerateDaysBetweenDates(moment(new Date()).startOf("year"), moment(new Date()).endOf("year"))
    async.eachSeries(monthArray, function(month , cb) {
        var value = {
            startDate: getFormattedStartDate(month),
            endDate: getFormattedEndDate(moment(month).endOf("month")),
            branchId : req.params.branchId ? req.params.branchId : null
        }
        var inventoryData = {};

        async.parallel({
            inventory : function(cbl) {
                reportOpertaion.getAllInventriesCountByBranchId(value, function(err, result) {
                    if (err) {
                        logger.error(err);
                    }
                    inventoryData = {
                        date : month,
                        data : result
                    };
                    cbl(null, []);
                })
            },
            target : function(cbl) {
                getTargetCountByBranchId({id : req.params.branchId, startDate : getFormattedStartMonth(month),
                endDate: getFormattedEndDate(month)}, function(err, result){
                    if(err) {
                        logger.error(err);
                    }
                    inventoryData.target = result;
                    cbl(null,[]);
                });
            }

        }, function(err, rslt){
            summaryArr.push(inventoryData);
            cb(null, []);
        })

     }, function(err, result) {
         if(err) {
            logger.error(err);
         }
         res.json(doSomeProcessing(summaryArr.slice(0)));
     });
}

function getFormattedStartDate(date){
   return moment(new Date(date)).startOf("day").format("YYYY-MM-DD HH:mm:ss");
}

function getFormattedEndDate(date){
    return moment(new Date(date)).endOf("day").format("YYYY-MM-DD HH:mm:ss");
 }

 function getFormattedStartMonth(date){
    return moment(new Date(date)).startOf("month").format("YYYY-MM-DD HH:mm:ss");
 }

 function getFormattedEndMonth(date){
     return moment(new Date(date)).endOf("month").format("YYYY-MM-DD HH:mm:ss");
  }


 // Returns an array of dates between the two dates
function enumerateDaysBetweenDates(startDate, endDate) {
    startDate = moment(startDate);
    endDate = moment(endDate);

    var now = startDate, dates = [];

    while (now.isBefore(endDate) || now.isSame(endDate)) {
        dates.push(now.format('YYYY-MM-DD'));
        now.add(1, 'months');
    }
    return dates;
};

function doSomeProcessing(data) {
    try {
        var out = [];
        data.map(function(datum, i) {
            var obj = {
                date : datum.date,
                item : []
            };

            var dataArr = datum.data;
            var targetArr = datum.target;
            for(var i = 0; i< dataArr.length; i++) {
                var bp_id = dataArr[i].branch_product_id;
                var product_name = dataArr[i].product_name;
                var quantity = dataArr[i].total_quantity;
                var target = _.find(targetArr, {branch_product_id : bp_id});
                var targetVal = 0;
                if(target) {
                    targetVal = target.target;
                };
                obj.item.push({
                    product_name,
                    target: targetVal,
                    quantity
                })
            }
            out.push(obj);
        });

        return out;
    } catch(error) {
        logger.error(err);
        return {};
    }
}

exports.fetchSummaryCount = (req, res, next) => {

    var staffCount_sql = "SELECT count(*) as staffCount FROM `maithree-db`.member where is_admin = 'Y' and active = 'Y'";
    var branchCount_sql = "SELECT count(*) as branchCount FROM `maithree-db`.branch where active = 'Y'";
    var studentsCount_sql = "SELECT count(student_id) as studentsCount FROM `maithree-db`.student_details";
    var productsCount_sql = "SELECT count(*) as productsCount FROM `maithree-db`.product_master where is_activity != NULL OR is_activity != 'Y'";

    var summary_data = [];

    async.series([
        function(callback) {
            executeQuery(branchCount_sql, function (data) {
                summary_data.push ({
                    "name" : "Branches",
                    "value" : "branches",
                    "count" : data[0].branchCount
                })
                callback(null);
            });
        },
        function(callback) {
            executeQuery(staffCount_sql, function (data) {
                summary_data.push ({
                    "name" : "Staffs",
                    "value" : "staffs",
                    "count" : data[0].staffCount
                })
                callback(null);
            });
        },
        function(callback) {
            executeQuery(studentsCount_sql, function (data) {
                summary_data.push ({
                    "name" : "Students",
                    "value" : "students",
                    "count" : data[0].studentsCount
                })
                callback(null);
            });
        },
        function(callback) {
            executeQuery(productsCount_sql, function (data) {
                summary_data.push ({
                    "name" : "Products",
                    "value" : "Products",
                    "count" : data[0].productsCount
                })
                callback(null);
            });
        }, function (err, results) {
            console.log(JSON.stringify(summary_data));
            res.json(summary_data);
        }
    ])
}

exports.productSummaryReport = (req,res, next) => {

    const sql_date_format = 'YYYY-MM-DD';

    const original_date = moment();

    var sql = "SELECT tracking.date as submittedDate,branch.name as branchName, prod.id as productId, prod.product_name as productName, steps.id as taskId, task_name as taskName, SUM(target) as taskTarget, SUM(completed) as taskCompleted from `maithree-db`.product_master_steps steps JOIN `maithree-db`.product_master prod ON steps.product_master_id = prod.id \
        JOIN `maithree-db`.student_task_mapping_details stud_task ON stud_task.product_master_id = prod.id AND stud_task.product_master_steps_id = steps.id \
        JOIN (select * from `maithree-db`.student_task_tracking where date >= ? AND date <= ?) tracking ON stud_task.mapping_id = tracking.student_task_mapping_details_mapping_id \
        JOIN `maithree-db`.`student_details` stud_details ON stud_task.student_details_student_id = stud_details.student_id \
        JOIN `maithree-db`.`branch-product_master` bp_master ON stud_task.product_master_id = bp_master.product_id AND stud_details.branch_id = bp_master.branch_id \
        JOIN `maithree-db`.`branch` branch ON bp_master.branch_id = branch.id \
        group by tracking.date, branchName, productId, taskId order by tracking.date ASC, branchName ASC, productId ASC, taskId ASC";

    var startDate = req.query.startDate ? moment(req.query.startDate) : original_date.clone().subtract(30, "days");
    var endDate = req.query.endDate ? moment(req.query.endDate) : original_date.clone();
    console.log(startDate, endDate)
    if (!startDate.isValid() || !endDate.isValid()) {
        return res.status(400).json({ error: "Input dates are not valid" });
    }

    if (startDate.isAfter(endDate) || endDate.isAfter(moment.now())) {
        return res.status(400).json({ error: "Please check the date range properly" });
    }

    if(endDate.diff(startDate, 'days') > 31) {
        return res.status(400).json({ error: "Maximum Date range is 31 days"});
    }

    try {

        logger.info(`Get Product Summary Report for the duration ::: Start Date =  ${startDate}, End Date = ${endDate}`);
        console.log(sql)
        db.query(sql, [startDate.format(sql_date_format), endDate.format(sql_date_format)], (err, result) => {
            if (err) {
                logger.error(err);
                return res.status(500).json({ error: "Unable to fetch Product Summary Report" });
            }
            else {
                return res.json(result);
            }
        });
    } catch (err) {
        logger.error(err);
        next(err);
    }
}

function executeQuery(sqlQuery, cb) {
    try {

        db.query(sqlQuery, function (err, result) {
            if (err) {
                logger.error(err);
                cb(err);
            }
            cb(result);
        });
    } catch (err) {
        cb(err);
    }
}