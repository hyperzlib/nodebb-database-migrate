// please add follow in index.js
// 请在index.js中加上这几行
program
	.command('transfer-database', null, {
		noHelp: true,
	})
	.alias('transDb')
	.description('Transfer database')
	.action(function () {
		require('./transfer-database').transDb(function (err) {
			if (err) {
				throw err;
			}
			console.log('OK'.green);
			process.exit();
		});
	});
