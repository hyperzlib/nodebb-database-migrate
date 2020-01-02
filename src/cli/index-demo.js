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