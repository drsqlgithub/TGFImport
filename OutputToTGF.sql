USE GraphDemo
GO
--parameters values for this database: https://docs.microsoft.com/en-us/sql/relational-databases/graphs/sql-graph-sample?view=sql-server-ver15

--list of nodes in format schema.nodetable.nameForLabel;schema.nodetable.nameForLabel;
--done this way because it is a lot easier to manually edit
DECLARE @NodeList nvarchar(4000) = 'dbo.person.name;dbo.Restaurant.name;dbo.City.name'
DECLARE @NodeList nvarchar(4000) = 'dbo.person.name;dbo.city.name'

--list of edges in format schema.edgeTable
DECLARE @EdgeList nvarchar(4000) = 'dbo.friendOf;dbo.likes;dbo.livesIn;dbo.locatedIn;'


--used to determine formatting of name in output
DECLARE @DefaultNodeType nvarchar(100) = '?' --I want to output them all 
DECLARE @DefaultEdgeType nvarchar(100) = '?'
DECLARE @LabelNonDefaultEdgeFlag bit = 1


--if node or edge type doesn't match this value exactly, the node will be named NameForLabel (NodeType) and the edge will 
--not have a label if it matches

DECLARE @NodeTableList table (SchemaName sysname, TableName sysname, NodeNameColumn sysname PRIMARY KEY (SchemaName, TableName))
DECLARE @EdgeTableList table (SchemaName sysname, TableName sysname, EdgeNameColumn sysname NULL PRIMARY KEY (SchemaName, TableName))

SET NOCOUNT ON;
DECLARE @crlf nvarchar(2) = CHAR(13) + CHAR(10)

--parse the two strings and put into tables
INSERT INTO @NodeTableList(SchemaName, TableName, NodeNameColumn)
SELECT PARSENAME(value,3), PARSENAME(value,2), PARSENAME(value,1)
FROM   STRING_SPLIT(@NodeList,';')
WHERE  PARSENAME(value,1) IS NOT NULL 

INSERT INTO @EdgeTableList(SchemaName, TableName)
SELECT PARSENAME(value,2), PARSENAME(value,1)
FROM   STRING_SPLIT(@EdgeList,';')
WHERE  PARSENAME(value,1) IS NOT NULL 
    

--create table to hold the nodes and edges. Nodes and edges each have their own id
--sequence, but we need them to be unique. Hence I added an identity column to node
DROP TABLE IF EXISTS #NodeOutput, #EdgeOutput
CREATE TABLE #NodeOutput
(
	NodeOutputId int IDENTITY PRIMARY KEY,
	NodeSchema sysname,
	NodeTable  sysname,
	NodeId     int,
	NodeName varchar(100),
	UNIQUE (NodeSchema, NodeTable, NodeId)
)

CREATE TABLE #EdgeOutput
(
	EdgeSchema varchar(1000),
	EdgeTable  varchar(1000),
	FromNodeOutputId int NULL,
	ToNodeOutputId int NULL,
	EdgeName	 varchar(100)
)

DECLARE @NodeCursor CURSOR,
		@EdgeCursor CURSOR,
		@NodeName sysname,
		@EdgeName sysname,
		@SchemaName sysname,
		@NodeNameColumn sysname,
		@SQLQuery nvarchar(MAX)

--cursoring over the different nodes and adding them with dynamic SQL
SET @NodeCursor = CURSOR FOR (SELECT SchemaName,TableName,NodeNameColumn FROM @NodeTableList)
OPEN @NodeCursor

WHILE 1=1
 BEGIN
	FETCH NEXT FROM @NodeCursor INTO @SchemaName, @NodeName,@NodeNameColumn
	IF @@FETCH_STATUS <> 0
	  BREAK

	--fetching the id from the JSON for the pseudocolumn $node_id
	SELECT @SQLQuery = 'INSERT INTO #NodeOutput (NodeSchema, NodeTable, NodeId, NodeName)' + @crlf + 
		   'SELECT ''' + REPLACE(@SchemaName,'''','''''') + ''', '''+ REPLACE(@NodeName,'''','''''') + ''', JSON_VALUE(CAST($node_id AS nvarchar(1000)),''$.id''), ' 
		   + QUOTENAME(@NodeNameColumn) + ' + ' +  CASE WHEN @DefaultNodeType = @NodeName OR @LabelNonDefaultEdgeFlag = 0 THEN '''''' ELSE ''' (' + REPLACE(@NodeName,'''','''''') + ')'''  END +
		   ' FROM ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@NodeName)

	EXEC (@SQLQuery)

 END;


SET @EdgeCursor = CURSOR FOR (SELECT SchemaName, TableName FROM @EdgeTableList)
OPEN @EdgeCursor 

WHILE 1=1
	BEGIN
		FETCH NEXT FROM @edgeCursor INTO @SchemaName, @EdgeName
		IF @@FETCH_STATUS <> 0
			BREAK
		
		--fetching the id from the JSON for the pseudocolumn $from and $to_id and using those values
		--to join to the #Node table I created to get the surrogate key for the output
		--
		SELECT @SQLQuery = 'WITH Parts AS (
 SELECT JSON_VALUE(CAST($from_id AS nvarchar(1000)),''$.schema'') AS FromNodeSchema, 
		JSON_VALUE(CAST($from_id AS nvarchar(1000)),''$.table'') AS FromNodeTable, 
		JSON_VALUE(CAST($from_id AS nvarchar(1000)),''$.id'') AS FromNodeId, 

		JSON_VALUE(CAST($To_id AS nvarchar(1000)),''$.schema'') AS ToNodeSchema, 
		JSON_VALUE(CAST($to_id AS nvarchar(1000)),''$.table'') AS ToNodeTable, 
		JSON_VALUE(CAST($to_id AS nvarchar(1000)),''$.id'') AS ToNodeId, 

        CASE WHEN ''' + REPLACE(@EdgeName,'''','''''') +''' <> ''' + REPLACE(@DefaultEdgeType,'''','''''') + ''' THEN ''' + REPLACE(@EdgeName,'''','''''') + ''' ELSE '''' END AS EdgeName 
 FROM ' + QUOTENAME(@SchemaName) + '.' + QUOTENAME(@EdgeName) + '
 )
 INSERT INTO #EdgeOutput(EdgeSchema, EdgeTable, FromNodeOutputId, ToNodeOutputId, EdgeName)
 SELECT ''' + REPLACE(@SchemaName,'''','''''') + ''' AS EdgeSchema,
		''' + REPLACE(@EdgeName,'''','''''') + ''' as EdgeName,
		FromNodeOutput.NodeOutputId AS FromNodeOutputId, 
	    ToNodeOutput.NodeOutputId AS ToNodeOutputId, 
		EdgeName
 FROM   Parts
		 JOIN #NodeOutput AS FromNodeOutput
			ON FromNodeOutput.NodeSchema = Parts.FromNodeSchema
			  AND FromNodeOutput.NodeTable = Parts.FromNodeTable
			  AND FromNodeOutput.NodeId = Parts.FromNodeId
		 JOIN #NodeOutput AS ToNodeOutput
			ON ToNodeOutput.NodeSchema = Parts.ToNodeSchema
			  AND ToNodeOutput.NodeTable = Parts.ToNodeTable
			  AND ToNodeOutput.NodeId = Parts.ToNodeId'

		
		EXEC (@SQLQuery)
	END;
GO



--after this, build the output into a temp table for ordering purposes
DECLARE @Output table (Ordering int IDENTITY, outputValue nvarchar(1000))

--get the nodes
INSERT INTO @Output(outputValue)
SELECT CONCAT(#NodeOutput.NodeOutputId, ' ', NodeName) FROM #NodeOutput

--add the separator
INSERT INTO @Output(outputValue)
SELECT '#'

--get the edges and their names
INSERT INTO @Output(outputValue)
SELECT CONCAT(#EdgeOutput.FromNodeOutputId, ' ', #EdgeOutput.ToNodeOutputId, ' ',#EdgeOutput.EdgeName) FROM #EdgeOutput

--return the output value, which I am pasting into a file for simplicity sake. Could easily be automated, but this was
--easy enough
SELECT [@Output].outputValue
FROM   @Output
ORDER BY [@Output].Ordering
