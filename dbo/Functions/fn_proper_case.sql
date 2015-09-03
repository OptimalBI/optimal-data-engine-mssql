CREATE FUNCTION [dbo].[fn_proper_case](@Text as varchar(1000))
returns varchar(1000)
as
begin
   declare @Ret varchar(1000);
   set @Ret = lower(@Text)
   set @ret = 
   replace(replace(replace(replace(replace(replace(replace(
replace(replace(replace(replace(replace(replace(replace(
replace(replace(replace(replace(replace(replace(replace(
replace(replace(replace(replace(replace(
' '+@Ret,
' a',' A'),' b',' B'),' c',' C'),' d',' D'),' e',' E'),' f',' F'),
' g',' G'),' h',' H'),' i',' I'),' j',' J'),' k',' K'),' l',' L'),
' m',' M'),' n',' N'),' o',' O'),' p',' P'),' q',' Q'),' r',' R'),
' s',' S'),' t',' T'),' u',' U'),' v',' V'),' w',' W'),' x',' X'),
' y',' Y'),' z',' Z')
   return @Ret
end