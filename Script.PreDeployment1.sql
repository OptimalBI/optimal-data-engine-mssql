/* Drop SBLogin if exists already, i.e. not the first installation of ODE  */

If EXISTS (SELECT [name] FROM master.sys.sql_logins WHERE [name] = 'SBLogin')
	DROP LOGIN [SBLogin];