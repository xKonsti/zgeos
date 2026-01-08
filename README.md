# ZGEOS

A Zig binding for the GEOS (Geometry Engine Open Source) library. This library provides geometric algorithms and operations through a Zig interface.

## Installation

Fetch this library using the Zig package manager:

```bash
zig fetch --save git+https://github.com/xKonsti/zgeos
```

This will automatically add the dependency to your `build.zig.zon` and resolve the correct hash.

Then in your `build.zig`:

```zig
const zgeos = b.dependency("zgeos", .{
    .target = target,
    .optimize = optimize,
});

exe.linkLibrary(zgeos.artifact("geos"));
exe.addModule("zgeos", zgeos.module("zgeos"));
```

## Usage Example

Here's a simple example of using ZGEOS to create and manipulate geometries:

```zig
const std = @import("std");
const zgeos = @import("zgeos");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create a coordinate
    var coord = try zgeos.Coordinate.init(10.0, 20.0);
    defer coord.deinit();

    // Create a point geometry
    var point = try zgeos.Point.init(coord, allocator);
    defer point.deinit();

    // Use the geometry
    const x = point.getX();
    const y = point.getY();

    std.debug.print("Point: ({}, {})\n", .{ x, y });
}
```

## Features

- Geometry creation and manipulation
- Spatial operations
- Coordinate handling
- Various geometric algorithms

## Building

```bash
zig build
```

## License

This is a Zig binding for GEOS. Please refer to the GEOS library license for details.
