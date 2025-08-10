/*
 * Copyright 2014-2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package version

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Version struct {
	Major, Minor, Patch, Build int
}

// Pattern to match semantic version: major.minor.patch.build
var pattern = `^(?P<major>\d+)(?:\.(?P<minor>\d+))?(?:\.(?P<patch>\d+))?(?:\.(?P<build>\d+))?(?:[-_\.~]*?(?P<suffix>.+))?$`
var regex = regexp.MustCompile(pattern)

// Parse creates a new Version from a semantic version string
func Parse(versionStr string) (*Version, error) {
	// Since it is common for versions in go to have 'v' prefixed, remove any leading 'v' prefix
	// just in case the version string is prefixed with 'v'. However, having said that
	// it is unlikely that the version string will have 'v' prefixed since the version is
	// coming from the server. This can be in handy if version is used in other contexts.
	versionStr = strings.TrimPrefix(versionStr, "v")

	matches := regex.FindStringSubmatch(versionStr)
	if matches == nil {
		return nil, fmt.Errorf("invalid version format: %s", versionStr)
	}

	fields := regex.SubexpNames()
	version := &Version{}
	for i, field := range fields {
		if i == 0 || field == "" { // skip full‑match at m[0]
			continue
		}

		switch field {
		case "major":
			major, err := strconv.Atoi(matches[i])
			if err != nil {
				return nil, fmt.Errorf("invalid %s version number: %s in %s", field, matches[i], versionStr)
			} else {
				version.Major = major
			}
		case "minor":
			minor, err := strconv.Atoi(matches[i])
			if err != nil {
				return nil, fmt.Errorf("invalid %s version number: %s in %s", field, matches[i], versionStr)
			} else {
				version.Minor = minor
			}
		case "patch":
			patch, err := strconv.Atoi(matches[i])
			if err != nil {
				return nil, fmt.Errorf("invalid %s version number: %s in %s", field, matches[i], versionStr)
			} else {
				version.Patch = patch
			}
		case "build":
			build, err := strconv.Atoi(matches[i])
			if err != nil {
				version.Build = 0
			} else {
				version.Build = build
			}
		}
	}

	return version, nil
}

// String returns the string representation of the version
func (v *Version) String() string {
	return fmt.Sprintf("%d.%d.%d.%d", v.Major, v.Minor, v.Patch, v.Build)
}

// Compare compares this version with another version
// Returns: -1 if this version is smaller, 0 if equal, 1 if this version is greater
func (v *Version) Compare(other *Version) int {
	if v.Major != other.Major {
		if v.Major < other.Major {
			return -1
		}
		return 1
	}

	if v.Minor != other.Minor {
		if v.Minor < other.Minor {
			return -1
		}
		return 1
	}

	if v.Patch != other.Patch {
		if v.Patch < other.Patch {
			return -1
		}
		return 1
	}

	if v.Build != other.Build {
		if v.Build < other.Build {
			return -1
		}
		return 1
	}

	return 0
}

// IsGreater returns true if this version is greater than the other version
func (v *Version) IsGreater(other *Version) bool {
	return v.Compare(other) > 0
}

// IsSmaller returns true if this version is smaller than the other version
func (v *Version) IsSmaller(other *Version) bool {
	return v.Compare(other) < 0
}

// IsEqual returns true if this version is equal to the other version
func (v *Version) IsEqual(other *Version) bool {
	return v.Compare(other) == 0
}

// IsGreaterOrEqual returns true if this version is greater than or equal to the other version
func (v *Version) IsGreaterOrEqual(other *Version) bool {
	return v.Compare(other) >= 0
}

// IsSmallerOrEqual returns true if this version is smaller than or equal to the other version
func (v *Version) IsSmallerOrEqual(other *Version) bool {
	return v.Compare(other) <= 0
}
