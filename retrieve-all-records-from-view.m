let Pagination = List.Skip(List.Generate( () => [Last_Key = "init", Counter=0],
 each [Last_Key] <> null,
 each [
   Last_Key = try if [Counter]<1 then ""
   else
     [WebCall][Value][offset] otherwise null,
   WebCall = try if [Counter]<1
   then
   Json.Document(Web.Contents("https://api.airtable.com",
     [RelativePath="v0/"&BASE_ID&"/"&TABLE_ID&"?view="&VIEW_ID&"&api_key="&API_KEY&""]))
   else
   Json.Document(Web.Contents("https://api.airtable.com",
     [RelativePath="v0/"&BASE_ID&"/"&TABLE_ID&"?view="&VIEW_ID&"&api_key="&API_KEY&"&offset="&Last_Key&""])),
   Counter = [Counter]+1
 ],
 each [WebCall]
),1),
 #"Converted to Table" = Table.FromList(
   Pagination, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
 #"Expanded Column1" = Table.ExpandRecordColumn(
   #"Converted to Table", "Column1", {"Value"}, {"Column1.Value"}),
 #"Expanded Column1.Value" = Table.ExpandRecordColumn(
   #"Expanded Column1", "Column1.Value", {"records"}, {"Column1.Value.records"}),
 #"Expanded Column1.Value.records" = Table.ExpandListColumn(
   #"Expanded Column1.Value", "Column1.Value.records"),
 #"Expanded Column1.Value.records1" = Table.ExpandRecordColumn(
   #"Expanded Column1.Value.records", "Column1.Value.records",
   {"id", "fields", "createdTime"},
   {"Column1.Value.records.id", "Column1.Value.records.fields", "Column1.Value.records.createdTime"}),
 #"Renamed Columns" = Table.RenameColumns(
   #"Expanded Column1.Value.records1",{{"Column1.Value.records.id", "_airtableRecordId"},
   {"Column1.Value.records.createdTime", "_airtableRecordCreatedAt"},
   {"Column1.Value.records.fields", "_airtableRecordFields"}}),
 #"Reordered Columns" = Table.ReorderColumns(
   #"Renamed Columns",
   {"_airtableRecordId", "_airtableRecordCreatedAt", "_airtableRecordFields"}),
 #"Expanded Record Fields" = Table.ExpandRecordColumn(
   #"Reordered Columns", "_airtableRecordFields",
   Record.FieldNames(#"Reordered Columns"{0}[_airtableRecordFields]),
   Record.FieldNames(#"Reordered Columns"{0}[_airtableRecordFields]))
in
 #"Expanded Record Fields"
