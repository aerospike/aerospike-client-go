//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ecommerce

import "time"

// The demo catalogue, ported from the Java SDK's SeedData so the two suites
// report the same numbers. Every filter, dashboard row and count in the example
// is calibrated against these figures, so changing them changes the output.

// seedCustomers builds the customer set: 20 buyers with varied balances, which
// is what makes the dashboard's ordering interesting.
func seedCustomers() []*Customer {
	rows := []struct {
		id, name, email string
		limit, balance  int64
	}{
		{"C-100", "Alice Park", "alice@example.com", 500_000, 0},
		{"C-101", "Bob Chen", "bob.chen@example.com", 250_000, 8_499},
		{"C-102", "Carol Martinez", "carol.m@example.com", 100_000, 52_300},
		{"C-103", "David Kim", "dkim@example.com", 750_000, 124_000},
		{"C-104", "Eva Johnson", "eva.j@example.com", 300_000, 3_200},
		{"C-105", "Frank Liu", "frank.liu@example.com", 400_000, 97_550},
		{"C-106", "Grace Patel", "grace.p@example.com", 200_000, 15_800},
		{"C-107", "Henry Nguyen", "henry.n@example.com", 600_000, 210_000},
		{"C-108", "Isabel Torres", "isabel.t@example.com", 350_000, 44_200},
		{"C-109", "Jack Wilson", "jack.w@example.com", 150_000, 1_000},
		{"C-110", "Karen Zhang", "karen.z@example.com", 500_000, 180_750},
		{"C-111", "Leo Brown", "leo.b@example.com", 100_000, 67_300},
		{"C-112", "Mia Garcia", "mia.g@example.com", 800_000, 340_000},
		{"C-113", "Noah Davis", "noah.d@example.com", 200_000, 22_100},
		{"C-114", "Olivia Lee", "olivia.l@example.com", 450_000, 89_400},
		{"C-115", "Paul Robinson", "paul.r@example.com", 300_000, 5_600},
		{"C-116", "Quinn Miller", "quinn.m@example.com", 250_000, 38_900},
		{"C-117", "Ruby Anderson", "ruby.a@example.com", 550_000, 155_200},
		{"C-118", "Sam Thomas", "sam.t@example.com", 175_000, 11_750},
		{"C-119", "Tina Jackson", "tina.j@example.com", 400_000, 72_600},
	}
	out := make([]*Customer, 0, len(rows))
	for _, r := range rows {
		out = append(out, &Customer{
			ID:               r.id,
			Name:             r.name,
			Email:            r.email,
			CreditLimitCents: r.limit,
			BalanceCents:     r.balance,
		})
	}
	return out
}

// seedProducts builds the catalogue across ten departments. Stock and price are
// spread widely enough that the scan filter (stock > 100, price < $100) and the
// sale filter (stock > 250, price <= $50) each select a distinct slice.
func seedProducts() []*Product {
	rows := []struct {
		sku, name  string
		price, qty int64
	}{
		// Electronics
		{`SKU-TV55`, `55" 4K Smart TV`, 49_999, 20},
		{`SKU-TV65`, `65" OLED TV`, 129_999, 8},
		{`SKU-HP01`, `Wireless Headphones`, 7_999, 150},
		{`SKU-HP02`, `Noise-Cancelling Headphones`, 24_999, 60},
		{`SKU-SPK01`, `Bluetooth Speaker`, 3_499, 200},
		{`SKU-SPK02`, `Soundbar`, 19_999, 35},
		{`SKU-TAB01`, `10" Tablet`, 29_999, 45},
		{`SKU-TAB02`, `12" Pro Tablet`, 79_999, 15},
		{`SKU-LAP01`, `Ultrabook Laptop`, 89_999, 25},
		{`SKU-LAP02`, `Gaming Laptop`, 149_999, 10},
		{`SKU-PHN01`, `Smartphone 128GB`, 69_999, 40},
		{`SKU-PHN02`, `Smartphone 256GB`, 89_999, 30},
		{`SKU-CAM01`, `Mirrorless Camera`, 99_999, 12},
		{`SKU-CAM02`, `Action Camera`, 29_999, 80},
		{`SKU-DRN01`, `Camera Drone`, 59_999, 18},
		// Home & Kitchen
		{`SKU-CFM01`, `Espresso Machine`, 34_999, 50},
		{`SKU-CFM02`, `Drip Coffee Maker`, 4_999, 120},
		{`SKU-BLN01`, `High-Speed Blender`, 8_999, 90},
		{`SKU-AIR01`, `Air Fryer`, 6_999, 110},
		{`SKU-TOA01`, `Toaster Oven`, 5_499, 130},
		{`SKU-MIX01`, `Stand Mixer`, 27_999, 40},
		{`SKU-VAC01`, `Robot Vacuum`, 39_999, 55},
		{`SKU-VAC02`, `Cordless Vacuum`, 24_999, 70},
		{`SKU-PUR01`, `Air Purifier`, 14_999, 85},
		{`SKU-HUM01`, `Humidifier`, 3_999, 160},
		// Fitness & Outdoors
		{`SKU-BIK01`, `Mountain Bike`, 54_999, 15},
		{`SKU-BIK02`, `Road Bike`, 79_999, 8},
		{`SKU-TRD01`, `Treadmill`, 69_999, 12},
		{`SKU-YGA01`, `Yoga Mat Premium`, 2_999, 300},
		{`SKU-DUM01`, `Adjustable Dumbbell Set`, 24_999, 65},
		{`SKU-TNT01`, `4-Person Tent`, 12_999, 40},
		{`SKU-SLP01`, `Sleeping Bag`, 4_999, 180},
		{`SKU-HKB01`, `Hiking Boots`, 8_999, 120},
		{`SKU-KYK01`, `Inflatable Kayak`, 19_999, 22},
		{`SKU-FIT01`, `Fitness Tracker`, 4_999, 250},
		// Clothing
		{`SKU-JKT01`, `Waterproof Jacket`, 7_999, 140},
		{`SKU-JKT02`, `Down Parka`, 14_999, 75},
		{`SKU-SNK01`, `Running Shoes`, 9_999, 200},
		{`SKU-SNK02`, `Trail Running Shoes`, 11_999, 90},
		{`SKU-TSH01`, `Performance T-Shirt`, 1_999, 500},
		{`SKU-TSH02`, `Graphic Tee`, 1_499, 400},
		{`SKU-JNS01`, `Slim Fit Jeans`, 3_999, 250},
		{`SKU-JNS02`, `Relaxed Fit Jeans`, 3_499, 280},
		{`SKU-DRS01`, `Casual Dress`, 4_999, 150},
		{`SKU-HAT01`, `Baseball Cap`, 999, 600},
		// Books & Media
		{`SKU-BK001`, `Java Concurrency In Practice`, 3_499, 350},
		{`SKU-BK002`, `Designing Data-Intensive Apps`, 4_299, 280},
		{`SKU-BK003`, `Clean Code`, 3_199, 400},
		{`SKU-BK004`, `The Pragmatic Programmer`, 3_999, 320},
		{`SKU-BK005`, `Database Internals`, 4_999, 200},
		{`SKU-VG001`, `Strategy Game Deluxe`, 5_999, 100},
		{`SKU-VG002`, `Racing Sim`, 4_999, 80},
		{`SKU-VG003`, `RPG Adventure`, 5_999, 90},
		{`SKU-VG004`, `Puzzle Collection`, 1_999, 220},
		{`SKU-VG005`, `Sports Game 2026`, 5_999, 60},
		// Office & Tech Accessories
		{`SKU-MNT01`, `27" 4K Monitor`, 34_999, 30},
		{`SKU-MNT02`, `Ultrawide Monitor`, 49_999, 18},
		{`SKU-KBD01`, `Mechanical Keyboard`, 9_999, 110},
		{`SKU-KBD02`, `Ergonomic Keyboard`, 12_999, 70},
		{`SKU-MSE01`, `Wireless Mouse`, 3_999, 300},
		{`SKU-MSE02`, `Gaming Mouse`, 5_999, 140},
		{`SKU-WBC01`, `4K Webcam`, 7_999, 160},
		{`SKU-USB01`, `USB-C Hub`, 3_499, 250},
		{`SKU-CHG01`, `65W USB-C Charger`, 2_999, 350},
		{`SKU-SSD01`, `1TB Portable SSD`, 7_999, 95},
		{`SKU-SSD02`, `2TB Portable SSD`, 12_999, 50},
		{`SKU-HDD01`, `4TB External HDD`, 8_999, 75},
		{`SKU-PWR01`, `20000mAh Power Bank`, 2_999, 270},
		{`SKU-CBL01`, `USB-C Cable 3-Pack`, 999, 800},
		{`SKU-ADP01`, `Universal Travel Adapter`, 1_999, 400},
		// Pet Supplies
		{`SKU-PET01`, `Automatic Pet Feeder`, 5_999, 120},
		{`SKU-PET02`, `Pet Camera`, 4_999, 90},
		{`SKU-PET03`, `Dog Bed Large`, 3_999, 200},
		{`SKU-PET04`, `Cat Tower Deluxe`, 7_999, 80},
		{`SKU-PET05`, `Pet Grooming Kit`, 2_499, 300},
		// Garden
		{`SKU-GRD01`, `Cordless Lawn Mower`, 29_999, 25},
		{`SKU-GRD02`, `Garden Tool Set`, 3_499, 180},
		{`SKU-GRD03`, `LED Solar Lights 10-Pack`, 2_499, 350},
		{`SKU-GRD04`, `Portable Fire Pit`, 9_999, 45},
		{`SKU-GRD05`, `Patio Umbrella`, 5_999, 60},
		// Health & Personal Care
		{`SKU-HLT01`, `Electric Toothbrush`, 4_999, 180},
		{`SKU-HLT02`, `Digital Scale`, 2_499, 250},
		{`SKU-HLT03`, `Blood Pressure Monitor`, 3_999, 130},
		{`SKU-HLT04`, `Massage Gun`, 12_999, 55},
		{`SKU-HLT05`, `First Aid Kit`, 1_999, 400},
		// Toys & Games
		{`SKU-TOY01`, `Building Blocks 1000pc`, 3_999, 250},
		{`SKU-TOY02`, `RC Car`, 4_999, 110},
		{`SKU-TOY03`, `Board Game Collection`, 2_999, 300},
		{`SKU-TOY04`, `Science Kit for Kids`, 2_499, 180},
		{`SKU-TOY05`, `Jigsaw Puzzle 2000pc`, 1_499, 220},
		// Travel
		{`SKU-TRV01`, `Carry-On Suitcase`, 12_999, 60},
		{`SKU-TRV02`, `Packing Cube Set`, 1_999, 350},
		{`SKU-TRV03`, `Neck Pillow Memory Foam`, 1_499, 500},
		{`SKU-TRV04`, `Luggage Scale`, 999, 400},
		{`SKU-TRV05`, `Waterproof Dry Bag`, 1_499, 280},
	}
	out := make([]*Product, 0, len(rows))
	for _, r := range rows {
		out = append(out, &Product{SKU: r.sku, Name: r.name, PriceCents: r.price, StockQty: r.qty})
	}
	return out
}

// seedOrders builds the order history. Timestamps are relative to now, so the
// data reads as recent whenever the example runs.
func seedOrders() []*Order {
	now := time.Now().UnixMilli()
	const day = int64(86_400_000)

	rows := []struct {
		id, customer, sku string
		qty, total        int64
		status            string
		daysAgo           int64
	}{
		// Alice (C-100)
		{"ORD-1001", "C-100", "SKU-TV55", 1, 49_999, "CONFIRMED", -10},
		{"ORD-1002", "C-100", "SKU-HP01", 2, 15_998, "SHIPPED", -8},
		{"ORD-1003", "C-100", "SKU-BK001", 1, 3_499, "DELIVERED", -3},
		// Bob (C-101)
		{"ORD-1004", "C-101", "SKU-CFM02", 1, 4_999, "DELIVERED", -15},
		{"ORD-1005", "C-101", "SKU-TSH01", 2, 3_998, "CONFIRMED", -1},
		// Carol (C-102)
		{"ORD-1006", "C-102", "SKU-LAP01", 1, 89_999, "DELIVERED", -30},
		{"ORD-1007", "C-102", "SKU-KBD01", 1, 9_999, "DELIVERED", -25},
		{"ORD-1008", "C-102", "SKU-MSE01", 1, 3_999, "SHIPPED", -5},
		{"ORD-1009", "C-102", "SKU-SSD01", 1, 7_999, "CONFIRMED", -1},
		// David (C-103), a high spender
		{"ORD-1010", "C-103", "SKU-TV65", 1, 129_999, "DELIVERED", -60},
		{"ORD-1011", "C-103", "SKU-LAP02", 1, 149_999, "DELIVERED", -45},
		{"ORD-1012", "C-103", "SKU-CAM01", 1, 99_999, "SHIPPED", -10},
		{"ORD-1013", "C-103", "SKU-DRN01", 1, 59_999, "CONFIRMED", -2},
		{"ORD-1014", "C-103", "SKU-MNT02", 1, 49_999, "CONFIRMED", 0},
		// Eva (C-104)
		{"ORD-1015", "C-104", "SKU-YGA01", 1, 2_999, "DELIVERED", -20},
		// Frank (C-105)
		{"ORD-1016", "C-105", "SKU-BIK01", 1, 54_999, "DELIVERED", -40},
		{"ORD-1017", "C-105", "SKU-HKB01", 1, 8_999, "SHIPPED", -7},
		{"ORD-1018", "C-105", "SKU-FIT01", 1, 4_999, "CONFIRMED", -1},
		// Grace (C-106)
		{"ORD-1019", "C-106", "SKU-PUR01", 1, 14_999, "DELIVERED", -12},
		{"ORD-1020", "C-106", "SKU-HUM01", 2, 7_998, "SHIPPED", -3},
		// Henry (C-107), a big spender
		{"ORD-1021", "C-107", "SKU-TAB02", 1, 79_999, "DELIVERED", -50},
		{"ORD-1022", "C-107", "SKU-PHN02", 2, 179_998, "DELIVERED", -30},
		{"ORD-1023", "C-107", "SKU-SPK02", 1, 19_999, "SHIPPED", -5},
		{"ORD-1024", "C-107", "SKU-WBC01", 1, 7_999, "CONFIRMED", 0},
		// Isabel (C-108)
		{"ORD-1025", "C-108", "SKU-VAC01", 1, 39_999, "DELIVERED", -18},
		{"ORD-1026", "C-108", "SKU-AIR01", 1, 6_999, "CONFIRMED", -2},
		// Jack (C-109)
		{"ORD-1027", "C-109", "SKU-HAT01", 3, 2_997, "DELIVERED", -5},
		// Karen (C-110)
		{"ORD-1028", "C-110", "SKU-BIK02", 1, 79_999, "DELIVERED", -35},
		{"ORD-1029", "C-110", "SKU-JKT01", 1, 7_999, "DELIVERED", -20},
		{"ORD-1030", "C-110", "SKU-SNK01", 2, 19_998, "SHIPPED", -4},
		{"ORD-1031", "C-110", "SKU-DRS01", 1, 4_999, "CONFIRMED", 0},
		// Leo (C-111)
		{"ORD-1032", "C-111", "SKU-VG001", 1, 5_999, "DELIVERED", -22},
		{"ORD-1033", "C-111", "SKU-VG003", 1, 5_999, "SHIPPED", -3},
		// Mia (C-112), the top spender
		{"ORD-1034", "C-112", "SKU-TV65", 1, 129_999, "DELIVERED", -55},
		{"ORD-1035", "C-112", "SKU-LAP02", 1, 149_999, "DELIVERED", -40},
		{"ORD-1036", "C-112", "SKU-PHN02", 1, 89_999, "DELIVERED", -25},
		{"ORD-1037", "C-112", "SKU-MNT01", 1, 34_999, "SHIPPED", -8},
		{"ORD-1038", "C-112", "SKU-KBD02", 1, 12_999, "SHIPPED", -3},
		{"ORD-1039", "C-112", "SKU-CHG01", 3, 8_997, "CONFIRMED", 0},
		// Noah (C-113)
		{"ORD-1040", "C-113", "SKU-BK002", 1, 4_299, "DELIVERED", -10},
		// Olivia (C-114)
		{"ORD-1041", "C-114", "SKU-TRD01", 1, 69_999, "DELIVERED", -28},
		{"ORD-1042", "C-114", "SKU-DUM01", 1, 24_999, "SHIPPED", -6},
		{"ORD-1043", "C-114", "SKU-TSH01", 3, 5_997, "CONFIRMED", -1},
		// Paul (C-115)
		{"ORD-1044", "C-115", "SKU-TOY01", 2, 7_998, "SHIPPED", -4},
		// Quinn (C-116)
		{"ORD-1045", "C-116", "SKU-PET01", 1, 5_999, "DELIVERED", -14},
		{"ORD-1046", "C-116", "SKU-PET03", 1, 3_999, "CONFIRMED", -1},
		// Ruby (C-117)
		{"ORD-1047", "C-117", "SKU-CAM01", 1, 99_999, "DELIVERED", -38},
		{"ORD-1048", "C-117", "SKU-SSD02", 1, 12_999, "DELIVERED", -20},
		{"ORD-1049", "C-117", "SKU-GRD01", 1, 29_999, "SHIPPED", -5},
		{"ORD-1050", "C-117", "SKU-HLT04", 1, 12_999, "CONFIRMED", 0},
		// Sam (C-118)
		{"ORD-1051", "C-118", "SKU-TRV01", 1, 12_999, "SHIPPED", -6},
		// Tina (C-119)
		{"ORD-1052", "C-119", "SKU-CFM01", 1, 34_999, "DELIVERED", -25},
		{"ORD-1053", "C-119", "SKU-BLN01", 1, 8_999, "DELIVERED", -15},
		{"ORD-1054", "C-119", "SKU-GRD03", 2, 4_998, "CONFIRMED", -2},
	}
	out := make([]*Order, 0, len(rows))
	for _, r := range rows {
		out = append(out, &Order{
			OrderID:    r.id,
			CustomerID: r.customer,
			SKU:        r.sku,
			Qty:        r.qty,
			TotalCents: r.total,
			Status:     r.status,
			Timestamp:  now + r.daysAgo*day,
		})
	}
	return out
}
