#######################################################################################
## Louis Davidsson drsql@hotmail.com
## Use at your own risk. Produces SQL script files from TGF files.

## Parameters
$directory = "E:\TGF Files\" #Location of the TGF files for importing
$outputDirectory = "E:\TGF Files Output\" #directory where the .SQL files are sent


#Configuration
$StagingDatabase = 'Tempdb' #Defaulted to tempdb so someone doesn't add objects to master or some other database and 
#it be my fault
$StagingSchema = 'NodeStaging'

$filter = "*.TGF" #used to limit to certain file names

#Debugging is easier with a lot of output, but mute normally
$VerboseMode = $True
$VerboseCountAnnounce = 100

#########################################################################################

$Files = Get-ChildItem $directory -Filter $Filter

"Processing directory: $directory for '.TGF' files"

if ($VerboseMode) {
    ""
    "Import start time: $(Get-Date)" 
}

for ($i = 0; $i -lt $files.Count; $i++) {

    $Request = $files[$i].FullName #file to be processed
    $BaseName = $files[$i].BaseName #filename without path or extension to identity data in import

    #filename should include NodeType-DefaultEdgeType-Unique, identifying info.tgf
    $NodeType, $EdgeType = $BaseName.split('-')[0,1] 
    
    #some output for testing
    if ($VerboseMode) {
        ""
        "Processing file:" + $Request
    }
    
    #sql file that will be put out for the import
    $OutputFile = $outputDirectory + $BaseName + '.SQL'
    
    #code to create the staging table if required. #filename is included so you can have > 1 copy of the same 
    #graph imported
    $WriteThis = 
    "
USE $StagingDatabase;
GO
SET NOCOUNT ON;
GO
--create the schema and tables if they have not been created in the schema you chose
IF NOT EXISTS (SELECT * FROM sys.schemas where name = '$StagingSchema')
    EXEC ('CREATE SCHEMA $StagingSchema')
GO
IF NOT EXISTS (SELECT * FROM sys.tables where object_id = OBJECT_ID('$StagingSchema.Node'))
    CREATE TABLE $StagingSchema.Node (Filename nvarchar(200) NOT NULL, NodeId int NOT NULL, Name nvarchar(100) NOT NULL, NodeType nvarchar(100) NOT NULL, PRIMARY KEY (FileName, NodeId))
GO
IF NOT EXISTS (SELECT * FROM sys.tables where object_id = OBJECT_ID('$StagingSchema.Edge'))
    CREATE TABLE $StagingSchema.Edge (Filename nvarchar(200) NOT NULL, FromNodeId int NOT NULL, ToNodeId int NOT NULL, EdgeType varchar(100) NULL)
GO
--delete previous data staged from this filename to let this run repeatedly
DELETE FROM $StagingSchema.Node WHERE Filename = '$BaseName'
GO
DELETE FROM $StagingSchema.Edge WHERE Filename = '$BaseName'
GO

--Nodes"

    #Write the start of the file, clobbering existing file 
    $WriteThis | Out-File -FilePath $OutputFile #-NoClobber

    $RowCount = 0; $Section = "Nodes"; #RowCount is just for progress monitoring if it is a very large file. 
    #The first section of the TGF file is the nodes. The second is edges, denoted by a row with "#"

    #read in the file, row by row
    $reader = [System.IO.File]::OpenText($Request)

    while ($null -ne ($line = $reader.ReadLine())) {
        #in the TGF file, it has nodes first, then edges. This changes us to edges when we reach #
        if ($line -eq '#') {
            if ($VerboseMode) {
                "Changing to Edges"
            }
            $Section = "Edges"            
            
            $WriteThis = "`r`n--Edges"
            $WriteThis | Out-File -FilePath $OutputFile -Append
        }
        else {
            $line = $line + " " * 100 #added 100 space characters to make the substring easier 
            if ($Section -eq "Nodes") {
                
                #pull the node name out of the string
                $NodeName = $line.Substring($line.indexOf(' '), 100 ).trim()
                #Make name safe for output if it has an ' in it
                $NodeName = $NodeName.Replace("'","''");

                $NodeType = $NodeType.Replace("'","''");

                #write the Node
                $WriteThis = "INSERT INTO $StagingSchema.Node (FileName, NodeId,Name,NodeType) `r`n" + 
                "VALUES ( '$BaseName'," + $line.Substring(0, $line.indexOf(' ')).trim() + ",'" + $NodeName + "','" + $NodeType + "');"

                $WriteThis | Out-File -FilePath $OutputFile -Append
            }
            else {
                #Write the Edge
                
                #Parsing this line is not as simple as using split because the third part of the line is 
                #the name and the name can have spaces (and the fields are split on space)

                #From Node is Simple, char 0 to first space
                $WriteFromNodeId = $line.Substring(0, $line.indexOf(' ')).trim()
                
                #get the value after the from node. it may or many not have additional information after it
                $AfterFromNode = $line.Substring($line.indexOf(' ') + 1, 100 )

                #pad for the substring
                $AfterFromNode = $AfterFromNode + " " * 100

                #Get the numeric surrogate of the from node for the insert
                $WriteToNodeId = $AfterFromNode.Substring(0, $AfterFromNode.indexOf(' ')).trim()
                
                #Fetch any additional data from the string, and trim it
                $AdditionalInfo = $AfterFromNode.Substring($line.indexOf(' ') + 1, 100 ).Trim()

                #if the data has no length, set it to NULL in the output
                if ($AdditionalInfo.Length -eq 0) {
                    $AdditionalInfo = 'NULL'
                }
                ELSE {
                    #otherwise, add single quotes and double up single quotes
                    $AdditionalInfo = "'" + $AdditionalInfo.Replace("'","''") + "'"
                }

                #double up single quotes and surround by a '
                $DefaultEdgeType = "'" + $EdgeType.Replace("'","''") + "'"

                #Edgetype is defaulted to the edge name, or the default
                $WriteEdgeType = "COALESCE (" + $AdditionalInfo.Trim() + "," + $DefaultEdgeType.Trim() + ")";

                #The script to output the edge
                $WriteThis = "INSERT INTO $StagingSchema.Edge (FileName, FromNodeId, ToNodeId, EdgeType) `r`n" + 
                "VALUES ('$BaseName'," + $WriteFromNodeId + "," + $WriteToNodeId.trim() + ", "  +  $WriteEdgeType + ");"

                #write this line
                $WriteThis | Out-File -FilePath $OutputFile -Append
            }
        }
        
        if ($VerboseMode) {
            if ((($Rowcount + 1) % $VerboseCountAnnounce) -eq 0) {
                "Processed " + $RowCount + " lines in the file"
            }
        }
        $RowCount++; 
    }
    $WriteThis = 
    "
GO
--Queries to output the nodes that have been created
SELECT *
FROM   NodeStaging.Node
WHERE  Node.Filename = '$BaseName'

SELECT *
FROM   NodeStaging.Edge
WHERE  Edge.Filename = '$BaseName'
ORDER BY Edge.FromNodeId,Edge.ToNodeId
GO
    "
    
    #Write the start of the file, clobbering existing file 
    $WriteThis | Out-File -FilePath $OutputFile -Append

    if ($VerboseMode) {
        "Finished file:" + $Request + " with " + $RowCount + " lines. Wrote script to " + $OutputFile
    }
}



if ($VerboseMode) {
    ""
    "Import End time:  $(Get-Date)" 
    ""
}