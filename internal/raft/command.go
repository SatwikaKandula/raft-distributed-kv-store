package raft

type CommandType string //Creates a new custom type called CommandType(like 'int' type).
// Its underlying data type is still string, but now Go treats it as a separate meaningful type.
const (
	//These are fixed values of type CommandType.
	CommandNoOp   CommandType = "NOOP"
	CommandPut    CommandType = "PUT"
	CommandGet    CommandType = "GET"
	CommandDelete CommandType = "DELETE"
)

type Command struct {
	//This represents one complete client command.
	Type  CommandType
	Key   string
	Value string
}
