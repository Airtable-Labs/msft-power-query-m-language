let 
    // Pagination Logic: This part handles pagination by making API calls to Airtable with different offsets to retrieve paginated data.
    Pagination = List.Skip(
        List.Generate(
            () => [Page_Key = "init", Counter=0], // Initialize page key and counter
            each [Page_Key] <> null, // Continue generating while Page_Key is not null
            each [
                Page_Key = try if [Counter] < 1 then "" else [WebCall][Value][offset] otherwise null, // Determine the next Page_Key
                WebCall = try if [Counter] < 1 then
                    // Initial API call without offset
                    Json.Document(
                        Web.Contents(
                            "https://api.airtable.com",
                            [
                                RelativePath = "v0/" & BASE_ID & "/" & TABLE_ID,
                                Headers = [Authorization = "Bearer " & PERSONAL_ACCESS_TOKEN]
                            ]
                        )
                    )
                else
                    // Subsequent API calls with offset
                    Json.Document(
                        Web.Contents(
                            "https://api.airtable.com",
                            [
                                RelativePath = "v0/" & BASE_ID & "/" & TABLE_ID & "?offset=" & [WebCall][Value][offset],
                                Headers = [Authorization = "Bearer " & PERSONAL_ACCESS_TOKEN]
                            ]
                        )
                    ),
                Counter = [Counter] + 1 // Increment the counter for each iteration
            ],
            each [WebCall]
        ),
        1
    ),

    // Convert the paginated data into a table
    #"Converted to Table" = Table.FromList(
        Pagination, Splitter.SplitByNothing(), null, null, ExtraValues.Error
    ),

    // Expand and structure the paginated data
    #"Expanded Column1" = Table.ExpandRecordColumn(
        #"Converted to Table", "Column1", {"Value"}, {"Column1.Value"}
    ),
    #"Expanded Column1.Value" = Table.ExpandRecordColumn(
        #"Expanded Column1", "Column1.Value", {"records"}, {"Column1.Value.records"}
    ),
    #"Expanded Column1.Value.records" = Table.ExpandListColumn(
        #"Expanded Column1.Value", "Column1.Value.records"
    ),
    #"Expanded Column1.Value.records1" = Table.ExpandRecordColumn(
        #"Expanded Column1.Value.records", "Column1.Value.records",
        {"id", "fields", "createdTime"},
        {"Column1.Value.records.id", "Column1.Value.records.fields", "Column1.Value.records.createdTime"}
    ),

    // Rename columns to align with a specific naming convention.
    #"Renamed Columns" = Table.RenameColumns(
        #"Expanded Column1.Value.records1",
        {
            {"Column1.Value.records.id", "_airtableRecordId"},
            {"Column1.Value.records.createdTime", "_airtableRecordCreatedAt"},
            {"Column1.Value.records.fields", "_airtableRecordFields"}
        }
    ),

    // Reorder columns to the desired order.
    #"Reordered Columns" = Table.ReorderColumns(
        #"Renamed Columns",
        {"_airtableRecordId", "_airtableRecordCreatedAt", "_airtableRecordFields"}
    ),

    // Expand the record fields dynamically based on distinct field names, ensuring that all fields are expanded regardless of schema changes.
    #"Expanded Record Fields" = Table.ExpandRecordColumn(
        #"Reordered Columns", "_airtableRecordFields",
        List.Distinct(List.Combine(List.Transform(
            List.Transform(Table.ToRecords(#"Reordered Columns"), each Record.Field(_, "_airtableRecordFields")),
            each Record.FieldNames(_)
        ))),
        List.Distinct(List.Combine(List.Transform(
            List.Transform(Table.ToRecords(#"Reordered Columns"), each Record.Field(_, "_airtableRecordFields")),
            each Record.FieldNames(_)
        )))
    )
in
    #"Expanded Record Fields"
