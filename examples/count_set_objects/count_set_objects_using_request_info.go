package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go"
	shared "github.com/aerospike/aerospike-client-go/examples/shared"
)

func main() {
	runExample(shared.Client)
	log.Println("Example finished successfully.")
}

func runExample(client *as.Client) {
	replFactor, err := replicationFactor(client, *shared.Namespace)
	shared.PanicOnError(err)
	log.Println("Replication Factor:", replFactor)

	totalObjects, err := countSetObjects(client, *shared.Namespace, *shared.Set)
	shared.PanicOnError(err)
	log.Println("Total Objects:", totalObjects)

	totalUniqueObjects := totalObjects / replFactor
	log.Println("Total Unique Object Count:", totalUniqueObjects)
}

func countSetObjects(client *as.Client, ns, set string) (int, error) {
	const statKey = "objects"

	// get the list of cluster nodes
	nodes := client.GetNodes()

	infop := as.NewInfoPolicy()

	objCount := 0

	// iterate over nodes
N:
	for _, n := range nodes {
		cmd := fmt.Sprintf("sets/%s/%s", ns, set)
		info, err := n.RequestInfo(infop, cmd)
		if err != nil {
			return -1, err
		}
		vals := strings.Split(info[cmd], ":")
		for _, val := range vals {
			if i := strings.Index(val, statKey); i > -1 {
				cnt, err := strconv.Atoi(val[i+len(statKey)+1:])
				if err != nil {
					return -1, err
				}
				objCount += cnt
				continue N
			}
		}
	}

	return objCount, nil
}

func replicationFactor(client *as.Client, ns string) (int, error) {
	const statKey = "effective_replication_factor"

	// get the list of cluster nodes
	nodes := client.GetNodes()

	infop := as.NewInfoPolicy()

	replFactor := -1

	// iterate over nodes
N:
	for _, n := range nodes {
		cmd := fmt.Sprintf("namespace/%s", ns)
		info, err := n.RequestInfo(infop, cmd)
		if err != nil {
			return -1, err
		}
		vals := strings.Split(info[cmd], ";")
		// fmt.Println(vals)
		for _, val := range vals {
			if i := strings.Index(val, statKey); i > -1 {
				rf, err := strconv.Atoi(val[i+len(statKey)+1:])
				if err != nil {
					return -1, err
				}

				if replFactor == -1 {
					replFactor = rf
				} else if replFactor != rf {
					return -1, fmt.Errorf("Inconsistent replication factor for namespace %s in cluster", ns)
				}

				continue N
			}
		}
	}

	return replFactor, nil
}
