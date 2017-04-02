package paxospb

import (
	"fmt"

	"github.com/golang/protobuf/proto"
)

// WrapMessage wraps a union type of Message in a new Message without a
// destination.
func WrapMessage(msg proto.Message) Message {
	var msgTyp isMessage_Type
	switch t := msg.(type) {
	case *ViewChange:
		msgTyp = &Message_ViewChange{ViewChange: t}
	case *ViewChangeProof:
		msgTyp = &Message_ViewChangeProof{ViewChangeProof: t}
	case *Prepare:
		msgTyp = &Message_Prepare{Prepare: t}
	case *PrepareOK:
		msgTyp = &Message_PrepareOk{PrepareOk: t}
	case *Proposal:
		msgTyp = &Message_Proposal{Proposal: t}
	case *Accept:
		msgTyp = &Message_Accept{Accept: t}
	case *GloballyOrderedUpdate:
		msgTyp = &Message_GloballyOrderedUpdate{GloballyOrderedUpdate: t}
	case *ClientUpdate:
		msgTyp = &Message_ClientUpdate{ClientUpdate: t}
	default:
		panic(fmt.Sprintf("proto: unexpected type %T in wrapMessage", t))
	}
	return Message{Type: msgTyp}
}
