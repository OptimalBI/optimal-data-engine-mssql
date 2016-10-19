
CREATE Procedure [dv_scripting].[dv_build_snippet] 
(@input_string nvarchar(4000)
,@output_string nvarchar(4000) output
)
AS
BEGIN
set nocount on
declare @start_pos int,
		@open_pos int,
		@brcktcount int,
		@string_pos int,
		@current_row int,
		@parent int,
		@string nvarchar(4000),
		@command nvarchar(4000),
		@parm_definition nvarchar(500),
		@commandstring nvarchar(4000),
		@expression nvarchar(4000),
		@snippet nvarchar(4000),
		@resultvar nvarchar(4000)

declare @snippet_table TABLE 
       ([id] integer NOT NULL identity(1,1),
		parent int NULL,
		command	nvarchar(4000) NULL,
		expression nvarchar(4000) NULL,
		snippet nvarchar(4000) NULL)

--Initialise outer loop - works through the input string,
--						  writing each function call to a new row in the table variable.
--                        continues until reduced to single function calls, which can be executed.

set @string = ltrim(rtrim(@input_string))
set @string = right(@string, len(@string) - charindex('[dv_scripting].[dv_', @string,1)+1) --trim off any superfluous stuff from the head of the command - command muct start with a function!
set @start_pos = charindex('(', @string,1)
set @command = left(@string, @start_pos-1)
set @snippet = @string
insert @snippet_table select 0, @command,  @string, @snippet
select @current_row = SCOPE_IDENTITY()
set @open_pos = 1
--set @string = @snippet

while @current_row <= (select max([id]) from @snippet_table)
begin
    -- Initialise second loop - works through a single row, pulling out each first level function call, into a new row in the table
	
	set @start_pos = charindex('[dv_scripting].[dv_', @string,@open_pos )
	while @start_pos > 0
	begin
		set @open_pos =  charindex('(', @string,@start_pos)
		set @brcktcount = 1
		while @brcktcount > 0
		begin
			set @open_pos += 1
			if substring(@string,@open_pos, 1) = '(' set @brcktcount = @brcktcount + 1
			if substring(@string,@open_pos, 1) = ')' set @brcktcount = @brcktcount - 1
		end

		set @commandstring = substring(@string,@start_pos,@open_pos-@start_pos+ 1) 
		set @command = left(@commandstring,charindex('(', @commandstring,1)-1)
		set @snippet = left(right(@commandstring, len(@commandstring) - charindex('(', @commandstring, 1)), len(right(@commandstring, len(@commandstring) - charindex('(', @commandstring, 1))) -1)
		insert @snippet_table select @current_row, @command, @commandstring, @snippet
		set @start_pos = charindex('[dv_scripting].[dv_', @string,@open_pos )
	end
	set @open_pos = 0
	set @current_row = @current_row + 1
	select @string = snippet from @snippet_table where [id] = @current_row
end

-- Now work backwards through the table, replacing commands with code snippets
set @parm_definition = N'@codesnippet nvarchar(4000) OUTPUT'
select @commandstring = expression from @snippet_table where parent = 0
select @current_row = max([id]) from @snippet_table
select @parent = parent 
      ,@command = 'SELECT @codesnippet = ' + command + '(' + snippet + ')'
	  ,@expression = expression
	  from @snippet_table where [id] = @current_row
-- Loop through the scripts, replacing the snippets in the parent
while @current_row > 1
begin
	EXECUTE sp_executesql @command, @parm_definition, @codesnippet = @snippet OUTPUT
	--if its the master row, just replace the whole snippet
	update @snippet_table set snippet = replace(snippet,@expression, @snippet) where [id] = case when @parent = 0 then @current_row else @parent end
	set @current_row = @current_row -1
	select @parent = parent
	      ,@command = 'SELECT @codesnippet = ' + command + '(' + snippet + ')'
		  ,@expression = expression
		  from @snippet_table where [id] = @current_row
end
select @output_string = replace(snippet, '#', '''') from @snippet_table where parent = 0
END