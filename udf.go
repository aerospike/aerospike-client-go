package aerospike

// UDFInfo carries information about UDFs on the server
type UDF struct {
	Filename string
	Hash     string
	Language Language
}
