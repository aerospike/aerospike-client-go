// Package onetomany demonstrates a one-to-many relationship — an agent
// holds a set-like list of the ids of listings it owns, and each listing
// stores the id of its owning agent — kept consistent via RunInTransaction,
// so the relationship can be queried and mutated from either side without
// a torn write being visible in between.
//
// Source: the use-case-cookbook's onetomany/OneToManyRelationships.java.
// That file (and every other file in the cookbook) imports
// com.aerospike.client.sdk.TypedDataSet/TypedKey/TypedKeyList/
// TypedRecordStream and com.aerospike.mapper.tools.AeroMapper — none of
// which exist anywhere in the current aerospike-client-java-sdk source
// (only TypeSafeDataSet.java does). The cookbook predates a rename and
// wouldn't compile against the current SDK. So this package is not a
// translation of that file — it re-derives the same real capability
// (Session.doInTransaction/doInTransactionReturning, confirmed accurate
// against the current TransactionalSession.java) using this repo's
// already-established Go idiom (sdk.NewTypedDataSet[T], sdk.Key), the
// same as ecommerce/queryexamples/commonexamples.
package onetomany

import (
	"fmt"
	"time"
)

const (
	agentFirstNameBin    = "firstName"
	agentLastNameBin     = "lastName"
	agentEmailBin        = "email"
	agentPhoneBin        = "phone"
	agentRegisteredAtBin = "registeredAt"
	agentListingsBin     = "listings"

	listingAddressBin     = "address"
	listingCityBin        = "city"
	listingStateBin       = "state"
	listingZipBin         = "zip"
	listingURLBin         = "url"
	listingDateListedBin  = "dateListed"
	listingAgentIDBin     = "agentId"
	listingDescriptionBin = "description"
)

// Agent is a real-estate agent, keyed by ID in the "agents" dataset.
// ListingIDs is the set-like list of listing ids this agent owns.
type Agent struct {
	ID           int64     `as:",key"`
	FirstName    string    `as:"firstName"`
	LastName     string    `as:"lastName"`
	Email        string    `as:"email"`
	Phone        string    `as:"phone"`
	RegisteredAt time.Time `as:"registeredAt"`
	ListingIDs   []string  `as:"listings"`
}

func (a Agent) String() string {
	return fmt.Sprintf("Agent[%d, %s %s, %d listings]", a.ID, a.FirstName, a.LastName, len(a.ListingIDs))
}

// Listing is a property listing, keyed by ID in the "listings" dataset.
// AgentID points back to the one agent that owns it.
type Listing struct {
	ID          string    `as:",key"`
	Address     string    `as:"address"`
	City        string    `as:"city"`
	State       string    `as:"state"`
	Zip         string    `as:"zip"`
	URL         string    `as:"url"`
	DateListed  time.Time `as:"dateListed"`
	AgentID     int64     `as:"agentId"`
	Description string    `as:"description"`
}

func (l Listing) String() string {
	return fmt.Sprintf("Listing[%s, %s, %s, %s, agent=%d]", l.ID, l.Address, l.City, l.State, l.AgentID)
}
