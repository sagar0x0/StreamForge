package broker

import (
	"testing"
	"time"
)

func TestMetadataControllerSetAndGetLeader(t *testing.T) {
	mc := NewMetadataController()

	mc.SetLeader("orders", 0, 1)
	mc.SetLeader("orders", 1, 2)
	mc.SetLeader("orders", 2, 1)

	leader, ok := mc.GetLeader("orders", 0)
	if !ok || leader != 1 {
		t.Fatalf("expected leader 1 for partition 0, got %d", leader)
	}

	leader, ok = mc.GetLeader("orders", 1)
	if !ok || leader != 2 {
		t.Fatalf("expected leader 2 for partition 1, got %d", leader)
	}

	_, ok = mc.GetLeader("orders", 99)
	if ok {
		t.Fatal("should not find leader for nonexistent partition")
	}

	_, ok = mc.GetLeader("nonexistent", 0)
	if ok {
		t.Fatal("should not find leader for nonexistent topic")
	}

	all := mc.GetPartitionLeaders("orders")
	if len(all) != 3 {
		t.Fatalf("expected 3 partition leaders, got %d", len(all))
	}
}

func TestISRManagerAddAndRemove(t *testing.T) {
	isr := NewISRManager(200 * time.Millisecond)

	isr.AddReplica(0, 1)
	isr.AddReplica(0, 2)
	isr.AddReplica(0, 3)

	replicas := isr.GetISR(0)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 ISR members, got %d", len(replicas))
	}

	// Simulate broker 3 falling behind
	time.Sleep(300 * time.Millisecond)

	// Update broker 1 and 2 to keep them in sync
	isr.UpdateReplicaProgress(0, 1)
	isr.UpdateReplicaProgress(0, 2)

	// Check ISR — broker 3 should be removed
	isr.CheckISR()
	replicas = isr.GetISR(0)
	if len(replicas) != 2 {
		t.Fatalf("expected 2 ISR members after lag check, got %d", len(replicas))
	}

	// Broker 3 catches up
	isr.UpdateReplicaProgress(0, 3)
	replicas = isr.GetISR(0)
	if len(replicas) != 3 {
		t.Fatalf("expected 3 ISR members after catchup, got %d", len(replicas))
	}
}

func TestRebalanceCoordinator(t *testing.T) {
	coord := NewRebalanceCoordinator()

	// One consumer joins
	coord.JoinGroup("test-group", "consumer-1")
	group := coord.groups["test-group"]
	if len(group.Assignments["consumer-1"]) != 4 {
		t.Fatalf("expected 4 partitions for single consumer, got %d", len(group.Assignments["consumer-1"]))
	}

	// Second consumer joins → rebalance
	coord.JoinGroup("test-group", "consumer-2")
	group = coord.groups["test-group"]
	total := 0
	for _, parts := range group.Assignments {
		total += len(parts)
	}
	if total != 4 {
		t.Fatalf("expected 4 total partitions after rebalance, got %d", total)
	}

	if group.GenerationID != 2 {
		t.Fatalf("expected generation 2, got %d", group.GenerationID)
	}
}
