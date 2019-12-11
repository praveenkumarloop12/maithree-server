var express = require('express')

var branchRouter = express.Router()
var branchController = require('../controllers').branchController
var studentController = require('../controllers').studentController;

branchRouter.route('/')
.all(function(req,res,next) {
    console.log(' inside branch router call')
    next();
})
.get(branchController.getBranchList)
//.delete(branchController.deleteAllBranch)

branchRouter.route('/:id')
.all(function(req,res,next) {
    console.log(' inside branch router with branch id param')
    next();
})
.get(branchController.getBranchById)
//.delete(branchController.deleteBranchById)
//.put(branchController.updateBranchById)


branchRouter.route('/:id/teachers')
.all(function(req,res,next) {
    console.log(' inside branch router with branch id param to get teachers')
    next();
})
.get(branchController.getTeachersByBranchId);


branchRouter.route('/:id/products')
.all(function(req,res,next) {
    console.log(' inside branch router with branch id param to get products')
    next();
})
.get(branchController.getProductsByBranchId);

branchRouter.route('/:id/products')
.all(function(req,res,next) {
    console.log(' inside branch router with branch id param to insert products')
    next();
})
.post(branchController.createProductsByBranchId);

branchRouter.route('/:id/targets')
.all(function(req,res,next) {
    console.log(' inside branch router with branch id param to insert targets')
    next();
})
.post(branchController.createTargetsByBranchId);

// Get Student product mapping for a branch
/*
branchRouter.route('/:id/getStudentMapping')
.all(function(req,res,next) {
    console.log(' inside branch router with getStudentMapping ::: ', req.params.id);
    next();
})
.get(studentController.getStudentProductMappingDetailsForBranch)
*/

// Get Student details for a branch
branchRouter.route('/:id/getStudents')
.all(function(req,res,next) {
    console.log(' inside branch router with getStudents ::: ', req.params.id);
    next();
})
.get(studentController.getStudentsByBranch)

// Get Student product mapping for a branch
branchRouter.route('/:id/getProductsForStudent')
.all(function(req,res,next) {
    console.log(' inside branch router with getProductsForStudent ::: ', req.params.id);
    next();
})
.post(studentController.getProductsForStudent)

// Get Task details for a product
branchRouter.route('/:id/getTasksForProduct')
.all(function(req,res,next) {
    console.log(' inside branch router with getTasksForProduct ::: ', req.params.id);
    next();
})
.post(studentController.getTasksMappedForProduct)

// Get Task details for a product
branchRouter.route('/:id/studentProgress')
.all(function(req,res,next) {
    console.log(' Inside branch router with getStudentProgress for student ::: ', req.query.studentId);
    next();
})
.get(studentController.getStudentProgress)

// Get Task details for a product
branchRouter.route('/:id/saveStudentTracking')
.all(function(req,res,next) {
    console.log(' inside branch router with saveStudentTracking ::: ', req.params.id);
    next();
})
.post(studentController.saveStudentTrackingDetails)

module.exports = branchRouter;