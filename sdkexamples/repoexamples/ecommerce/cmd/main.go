// Command ecommerce runs the e-commerce example end to end: seed data,
// place an order, show error handling, stream a customer's orders, run the
// dashboard, exercise map ops, scan, apply a background sale, then re-scan.
package main

import (
	"context"
	"fmt"
	"log"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdkexamples/repoexamples/ecommerce"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// run holds every fallible step so a deferred cluster.Close() actually
// fires on error — log.Fatal directly in main would call os.Exit and skip
// it, defeating the exact connect/close lifecycle this example is meant to
// demonstrate.
func run(ctx context.Context) error {
	cluster, err := sdk.NewClusterDefinition("localhost", 3100).Connect(ctx)
	if err != nil {
		return err
	}
	defer cluster.Close()

	if err := cluster.Ping(ctx); err != nil {
		return err
	}

	session, err := cluster.CreateSession(ctx, sdk.DefaultBehavior())
	if err != nil {
		return err
	}

	customerDS, err := sdk.NewTypedDataSet[ecommerce.Customer]("test", "customers")
	if err != nil {
		return err
	}
	productDS, err := sdk.NewTypedDataSet[ecommerce.Product]("test", "products")
	if err != nil {
		return err
	}
	orderDS, err := sdk.NewTypedDataSet[ecommerce.Order]("test", "orders")
	if err != nil {
		return err
	}

	svc := ecommerce.NewService(session, customerDS, productDS, orderDS)

	if err := svc.Seed(ctx); err != nil {
		return err
	}

	fmt.Println("--- Placing order: customer=C-100, sku=SKU-LAP01, qty=1 ---")
	order, err := svc.PlaceOrder(ctx, "C-100", "SKU-LAP01", 1)
	if err != nil {
		log.Printf("PlaceOrder: %v", err)
	} else {
		fmt.Printf("Order placed: %s\n\n", order)
	}

	if err := svc.DemonstrateErrorHandling(ctx); err != nil {
		return err
	}

	if err := svc.StreamOrders(ctx, "C-100"); err != nil {
		return err
	}

	fmt.Println("--- Top-spender dashboard (batch query + where clause) ---")
	summaries, err := svc.ListTopSpenders(ctx)
	if err != nil {
		return err
	}
	for _, summary := range summaries {
		fmt.Printf("  %-18s  balance=$%8.2f  orders=%d  order_total=$%.2f\n",
			summary.Customer.Name, float64(summary.Customer.BalanceCents)/100.0,
			summary.OrderCount, float64(summary.OrderTotalCents)/100.0)
	}
	fmt.Println()

	fmt.Println("--- Map operations: product ratings for SKU-TV55 ---")
	if err := svc.RecordProductRatings(ctx); err != nil {
		return err
	}
	fmt.Println()

	fmt.Println("--- Scanning for products: stock > 100 AND price < $100 ---")
	if err := printAffordable(ctx, svc); err != nil {
		return err
	}

	fmt.Println("--- Background scan: applying sale prices (stock > 250, price <= $50) ---")
	if err := svc.ApplySalePrices(ctx); err != nil {
		return err
	}
	fmt.Println("  Sale prices applied.")
	fmt.Println()

	fmt.Println("--- Re-scanning for products: stock > 100 AND price < $100 ---")
	return printAffordable(ctx, svc)
}

func printAffordable(ctx context.Context, svc *ecommerce.Service) error {
	products, err := svc.ScanAffordableProducts(ctx)
	if err != nil {
		return err
	}
	for i, p := range products {
		sale := ""
		if p.IsOnSale() {
			sale = fmt.Sprintf("SALE $%.2f", float64(p.SalePriceCents)/100.0)
		}
		fmt.Printf("  [%2d] %-35s $%6.2f  stock=%-3d  %s\n",
			i+1, p.Name, float64(p.PriceCents)/100.0, p.StockQty, sale)
	}
	fmt.Printf("  Scan complete: %d matching products found.\n\n", len(products))
	return nil
}
