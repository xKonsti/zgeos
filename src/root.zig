const std = @import("std");
const assert = std.debug.assert;
const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;

const c = @cImport({
    @cInclude("geos_c.h");
});

const InternalGeometry = c.GEOSGeometry;

const messageHandlerType = c.GEOSMessageHandler_r;

fn errorMessageHandler(message: [*c]const u8, data: ?*anyopaque) callconv(.c) void {
    std.log.err("geos - {s} with data: {?}", .{ message, data });
}

/// GEOS context handle - provides thread-safe GEOS operations
pub const Context = struct {
    handle: *c.GEOSContextHandle_HS,

    /// Initialize a new GEOS context
    pub fn init() !Context {
        const handle = c.GEOS_init_r() orelse return error.GeosInitFailed;
        _ = c.GEOSContext_setErrorMessageHandler_r(handle, errorMessageHandler, null);
        return .{ .handle = handle };
    }

    /// Cleanup GEOS context - must be called when done with all geometries using this context
    pub fn deinit(self: *Context) void {
        c.GEOS_finish_r(self.handle);
    }
};

/// Default global context for convenient single-threaded use
var default_context: ?Context = null;
var default_context_mutex: std.Thread.Mutex = .{};

/// Initialize default global GEOS context - convenient for single-threaded programs
pub fn init() !void {
    default_context_mutex.lock();
    defer default_context_mutex.unlock();

    if (default_context != null) return;
    default_context = try Context.init();
}

/// Cleanup default global GEOS context
pub fn deinit() void {
    default_context_mutex.lock();
    defer default_context_mutex.unlock();

    if (default_context) |*ctx| {
        ctx.deinit();
        default_context = null;
    }
}

/// Get the default context, initializing if needed
fn getDefaultContext() !*Context {
    default_context_mutex.lock();
    defer default_context_mutex.unlock();

    if (default_context == null) {
        default_context = try Context.init();
    }
    return &default_context.?;
}

/// Coordinate in 2D or 3D space
pub const Coordinate = struct {
    x: f64,
    y: f64,
    z: ?f64 = null,

    pub inline fn is3D(self: Coordinate) bool {
        return self.z != null;
    }

    pub inline fn asPoint(self: Coordinate) Point {
        return Point.init(.{ .x = self.x, .y = self.y });
    }
};

const CoordList = []Coordinate;

/// Bounding box (axis-aligned rectangle)
pub const BoundingBox = struct {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,

    pub inline fn fromPoints(points: anytype) BoundingBox {
        var min_x = std.math.floatMax(f64);
        var min_y = std.math.floatMax(f64);
        var max_x = std.math.floatMin(f64);
        var max_y = std.math.floatMin(f64);

        for (points) |_p| {
            const p = if (@TypeOf(_p) == Point) _p.coord else _p;
            min_x = @min(min_x, p.x);
            min_y = @min(min_y, p.y);
            max_x = @max(max_x, p.x);
            max_y = @max(max_y, p.y);
        }
        return BoundingBox{
            .min_x = min_x,
            .min_y = min_y,
            .max_x = max_x,
            .max_y = max_y,
        };
    }

    pub inline fn width(self: BoundingBox) f64 {
        return @abs(self.max_x - self.min_x);
    }

    pub inline fn height(self: BoundingBox) f64 {
        return @abs(self.max_y - self.min_y);
    }

    pub inline fn center(self: BoundingBox) Coordinate {
        return .{
            .x = (self.min_x + self.max_x) / 2.0,
            .y = (self.min_y + self.max_y) / 2.0,
        };
    }

    pub inline fn contains(self: BoundingBox, coord: Coordinate) bool {
        return coord.x >= self.min_x and coord.x <= self.max_x and
            coord.y >= self.min_y and coord.y <= self.max_y;
    }

    pub inline fn intersects(self: BoundingBox, other: BoundingBox) bool {
        return !(self.max_x < other.min_x or self.min_x > other.max_x or
            self.max_y < other.min_y or self.min_y > other.max_y);
    }
};

/// Create a coordinate sequence from a slice of coordinates (uses bulk copy for better performance)
fn createCoordSeq(ctx: *Context, coords: []const Coordinate) !*c.GEOSCoordSequence {
    if (coords.len == 0) {
        const seq = c.GEOSCoordSeq_create_r(ctx.handle, 0, 2) orelse return error.CoordSeqCreateFailed;
        return seq;
    }

    const has_z = coords[0].is3D();
    const num_coords = coords.len;
    const buff_len = if (has_z) num_coords * 3 else num_coords * 2;

    const buffer = try std.heap.c_allocator.alloc(f64, buff_len);
    defer std.heap.c_allocator.free(buffer);

    for (coords, 0..) |coord, i| {
        const base_idx = if (has_z) i * 3 else i * 2;
        buffer[base_idx] = coord.x;
        buffer[base_idx + 1] = coord.y;

        if (has_z)
            buffer[base_idx + 2] = coord.z.?;
    }

    const seq = c.GEOSCoordSeq_copyFromBuffer_r(
        ctx.handle,
        buffer.ptr,
        @intCast(num_coords),
        @intFromBool(has_z),
        0,
    ) orelse return error.CoordSeqCreateFailed;
    return seq;
}

/// Read coordinates from a coordinate sequence
fn readCoordSeq(ctx: *Context, seq: *const c.GEOSCoordSequence, allocator: std.mem.Allocator) ![]Coordinate {
    var size: u32 = undefined;
    if (c.GEOSCoordSeq_getSize_r(ctx.handle, seq, &size) == 0) {
        return error.CoordSeqReadFailed;
    }

    var dims: u32 = undefined;
    if (c.GEOSCoordSeq_getDimensions_r(ctx.handle, seq, &dims) == 0) {
        return error.CoordSeqReadFailed;
    }

    const coords = try allocator.alloc(Coordinate, size);
    errdefer allocator.free(coords);

    for (0..size) |i| {
        const idx: u32 = @intCast(i);
        var x: f64 = undefined;
        var y: f64 = undefined;
        var z: f64 = undefined;

        if (c.GEOSCoordSeq_getXY_r(ctx.handle, seq, idx, &x, &y) == 0) {
            return error.CoordSeqReadFailed;
        }

        coords[i] = .{ .x = x, .y = y };

        if (dims >= 3) {
            if (c.GEOSCoordSeq_getZ_r(ctx.handle, seq, idx, &z) == 0) {
                return error.CoordSeqReadFailed;
            }
            coords[i].z = z;
        }
    }

    return coords;
}

/// Helper to create geometry collections - reduces code duplication
fn createCollection(
    ctx: *Context,
    allocator: std.mem.Allocator,
    geoms: []const *c.GEOSGeometry,
    collection_type: c_int,
) !*c.GEOSGeometry {
    const geom_copies = try allocator.alloc(*c.GEOSGeometry, geoms.len);
    defer allocator.free(geom_copies);

    for (geoms, 0..) |geom, i| {
        geom_copies[i] = c.GEOSGeom_clone_r(ctx.handle, geom) orelse return error.CloneFailed;
    }

    return c.GEOSGeom_createCollection_r(
        ctx.handle,
        collection_type,
        @ptrCast(geom_copies.ptr),
        @intCast(geom_copies.len),
    ) orelse error.CollectionCreateFailed;
}

/// Point geometry
pub const Point = struct {
    coord: Coordinate,

    /// Create a point from coordinates
    pub fn init(coord: Coordinate) Point {
        return .{ .coord = coord };
    }

    /// Get the coordinate of this point
    pub fn getCoordinate(self: Point) Coordinate {
        return self.coord;
    }

    /// Get X coordinate
    pub fn getX(self: Point) f64 {
        return self.coord.x;
    }

    /// Get Y coordinate
    pub fn getY(self: Point) f64 {
        return self.coord.y;
    }

    pub fn isEmpty(_: Point) bool {
        return false; // A point with coordinates is never empty
    }

    pub inline fn getBounds(self: Point) BoundingBox {
        return .{ .min_x = self.coord.x, .min_y = self.coord.y, .max_x = self.coord.x, .max_y = self.coord.y };
    }

    /// Convert to GEOS geometry (allocates - caller must deinit the returned Geometry)
    pub fn toGeosGeometry(self: Point, ctx: *Context) !*c.GEOSGeometry {
        const seq = try createCoordSeq(ctx, &[_]Coordinate{self.coord});
        const geom = c.GEOSGeom_createPoint_r(ctx.handle, seq) orelse return error.PointCreateFailed;

        // Validate the point was created correctly
        if (c.GEOSisValid_r(ctx.handle, geom) == 0) {
            c.GEOSGeom_destroy_r(ctx.handle, geom);
            return error.InvalidPointCreated;
        }

        return geom;
    }

    pub fn asGeometry(self: Point) Geometry {
        return .{ .point = self };
    }
};

/// LineString geometry
pub const LineString = struct {
    coords: CoordList,
    gpa: std.mem.Allocator,

    /// Create a linestring from coordinates (min 2 points)
    pub fn init(gpa: std.mem.Allocator, coords: []const Coordinate) !LineString {
        if (coords.len < 2) {
            return error.InsufficientCoordinates;
        }

        return .{
            .gpa = gpa,
            .coords = try gpa.dupe(Coordinate, coords),
        };
    }

    pub fn deinit(self: *LineString, gpa: std.mem.Allocator) void {
        gpa.free(self.coords);
    }

    /// Get number of points in linestring
    pub fn getNumPoints(self: LineString) usize {
        return self.coords.items.len;
    }

    /// Convert to GEOS geometry (allocates - caller must destroy with GEOSGeom_destroy_r)
    pub fn toGeosGeometry(self: LineString, ctx: *Context) !*c.GEOSGeometry {
        const seq = try createCoordSeq(ctx, self.coords);
        const geom = c.GEOSGeom_createLineString_r(ctx.handle, seq) orelse return error.LineStringCreateFailed;
        return geom;
    }

    pub fn asGeometry(self: LineString) Geometry {
        return .{ .linestring = self };
    }

    /// Clone this linestring (deep copy of coordinate data)
    pub fn clone(self: LineString, allocator: std.mem.Allocator) !LineString {
        return LineString.init(allocator, self.coords) catch |err| switch (err) {
            error.OutOfMemory => error.OutOfMemory,
            else => unreachable,
        };
    }
};

/// LinearRing geometry (closed linestring)
pub const LinearRing = struct {
    coords: CoordList,
    gpa: std.mem.Allocator, // null = not owned, non-null = owned

    /// Create a linear ring from coordinates (must be closed, min 3 points)
    pub fn init(gpa: std.mem.Allocator, coords: []const Coordinate) !LinearRing {
        if (coords.len < 3) return error.InsufficientCoordinates;

        if (!try coords[0].asPoint().asGeometry().equals(coords[coords.len - 1].asPoint().asGeometry()))
            return error.NotClosed;

        return .{
            .gpa = gpa,
            .coords = try gpa.dupe(Coordinate, coords),
        };
    }

    pub fn deinit(self: *LinearRing, gpa: std.mem.Allocator) void {
        gpa.free(self.coords);
    }

    /// Convert to GEOS geometry (allocates - caller must destroy with GEOSGeom_destroy_r)
    pub fn toGeosGeometry(self: LinearRing, ctx: *Context) !*c.GEOSGeometry {
        const seq = try createCoordSeq(ctx, self.coords);
        const geom = c.GEOSGeom_createLinearRing_r(ctx.handle, seq) orelse return error.LinearRingCreateFailed;
        return geom;
    }

    pub fn asGeometry(self: LinearRing) Geometry {
        return .{ .linearring = self };
    }

    /// Clone this linear ring (deep copy of coordinate data)
    pub fn clone(self: LinearRing, allocator: std.mem.Allocator) !LinearRing {
        return LinearRing.init(allocator, self.coords) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => unreachable,
        };
    }
};

/// Polygon geometry
pub const Polygon = struct {
    exterior: CoordList,
    holes: []CoordList,

    /// Create a polygon with exterior ring and optional holes
    pub fn init(gpa: std.mem.Allocator, exterior: []const Coordinate, holes: []const []const Coordinate) !Polygon {
        const poly_exterior = try gpa.dupe(Coordinate, exterior);
        errdefer gpa.free(poly_exterior);

        var poly_holes: std.ArrayList(CoordList) = try .initCapacity(gpa, holes.len);
        errdefer {
            for (poly_holes.items) |hole| {
                gpa.free(hole);
            }
            poly_holes.deinit(gpa);
        }

        for (holes) |hole|
            poly_holes.appendAssumeCapacity(try gpa.dupe(Coordinate, hole));

        return .{
            .exterior = poly_exterior,
            .holes = try poly_holes.toOwnedSlice(gpa),
        };
    }

    /// Create a simple polygon from exterior ring only (shortcut for polygons without holes)
    pub fn initFromExterior(alloc: std.mem.Allocator, exterior: []const Coordinate) !Polygon {
        return Polygon.init(alloc, exterior, &[_][]const Coordinate{});
    }

    pub fn deinit(self: *Polygon, gpa: std.mem.Allocator) void {
        gpa.free(self.exterior);

        for (self.holes) |hole| {
            gpa.free(hole);
        }
        gpa.free(self.holes);
    }

    pub inline fn getBounds(self: Polygon) BoundingBox {
        return BoundingBox.fromPoints(self.exterior);
    }

    pub fn exteriorRing(self: Polygon, gpa: std.mem.Allocator) !LinearRing {
        return try .init(gpa, self.exterior);
    }

    pub fn interiorRing(self: Polygon, gpa: std.mem.Allocator, i: usize) !LinearRing {
        return try .init(gpa, self.holes[i]);
    }

    pub fn getNumInteriorRings(self: Polygon) usize {
        return self.holes.len;
    }

    /// Convert to GEOS geometry (allocates - caller must destroy with GEOSGeom_destroy_r)
    pub fn toGeosGeometry(self: Polygon, ctx: *Context) !*c.GEOSGeometry {
        const alloc = std.heap.c_allocator;
        // Create exterior ring
        const shell_seq = try createCoordSeq(ctx, self.exterior);
        const shell = c.GEOSGeom_createLinearRing_r(ctx.handle, shell_seq) orelse return error.LinearRingCreateFailed;

        // Create holes
        const hole_geoms = try alloc.alloc(*InternalGeometry, self.holes.len);
        defer alloc.free(hole_geoms);

        for (self.holes, 0..) |hole_coords, i| {
            const hole_seq = try createCoordSeq(ctx, hole_coords);
            const hole = c.GEOSGeom_createLinearRing_r(ctx.handle, hole_seq) orelse return error.LinearRingCreateFailed;
            hole_geoms[i] = hole;
        }

        const geom: *InternalGeometry = c.GEOSGeom_createPolygon_r(
            ctx.handle,
            shell,
            if (hole_geoms.len > 0) @ptrCast(hole_geoms.ptr) else null,
            @intCast(hole_geoms.len),
        ) orelse return error.PolygonCreateFailed;

        // Validate polygon
        if (c.GEOSisValid_r(ctx.handle, geom) == 0) {
            //TODO: what is this?
            const reason_ptr = c.GEOSisValidReason_r(ctx.handle, geom);
            if (reason_ptr != null) {
                std.log.err("Invalid polygon: {s}", .{reason_ptr});
                c.GEOSFree_r(ctx.handle, reason_ptr);
            } else {
                std.log.err("Invalid polygon: unknown reason", .{});
            }
            c.GEOSGeom_destroy_r(ctx.handle, geom);
            return error.InvalidPolygonCreated;
        }

        return geom;
    }

    pub fn asGeometry(self: Polygon) Geometry {
        return .{ .polygon = self };
    }

    /// Clone this polygon (deep copy of exterior and all holes)
    pub fn clone(self: Polygon, alloc: std.mem.Allocator) !Polygon {
        return try .init(alloc, self.exterior, self.holes);
    }
};

/// MultiPoint geometry
pub const MultiPoint = struct {
    points: std.ArrayList(Point),

    /// Create a multipoint from points
    pub fn init(alloc: std.mem.Allocator, points: []const Point) !MultiPoint {
        var points_list = std.ArrayList(Point){};
        errdefer points_list.deinit(alloc);
        try points_list.appendSlice(alloc, points);
        return .{ .points = points_list };
    }

    /// Create from coordinates
    pub fn initFromCoords(alloc: std.mem.Allocator, coords: []const Coordinate) !MultiPoint {
        var points_list = std.ArrayList(Point){};
        errdefer points_list.deinit(alloc);

        for (coords) |coord| {
            try points_list.append(alloc, Point.init(coord));
        }
        return .{ .points = points_list, .alloc = alloc };
    }

    pub fn deinit(self: *MultiPoint, gpa: std.mem.Allocator) void {
        self.points.deinit(gpa);
    }

    pub inline fn getBounds(self: MultiPoint) BoundingBox {
        return BoundingBox.fromPoints(self.points.items);
    }

    pub fn getNumGeometries(self: MultiPoint) usize {
        return self.points.items.len;
    }

    /// Get a point by index
    pub fn getGeometry(self: MultiPoint, index: usize) !Point {
        if (index >= self.points.items.len) return error.IndexOutOfBounds;
        return self.points.items[index];
    }

    /// Convert to GEOS geometry (allocates - caller must destroy with GEOSGeom_destroy_r)
    pub fn toGeosGeometry(self: MultiPoint, ctx: *Context) !*c.GEOSGeometry {
        const geoms = try std.heap.page_allocator.alloc(*c.GEOSGeometry, self.points.items.len);
        defer std.heap.page_allocator.free(geoms);

        for (self.points.items, 0..) |pt, i| {
            geoms[i] = try pt.toGeosGeometry(ctx);
        }

        return try createCollection(ctx, std.heap.page_allocator, geoms, c.GEOS_MULTIPOINT);
    }

    pub fn asGeometry(self: MultiPoint) Geometry {
        return .{ .multipoint = self };
    }

    /// Clone this multipoint (deep copy of points array)
    pub fn clone(self: MultiPoint, gpa: std.mem.Allocator) !MultiPoint {
        return .{
            .points = try self.points.clone(gpa),
        };
    }
};

/// MultiLineString geometry
pub const MultiLineString = struct {
    linestrings: std.ArrayList(LineString),

    /// Create a multilinestring from linestrings
    pub fn init(gpa: std.mem.Allocator, linestrings: []const LineString) !MultiLineString {
        var ls_list = std.ArrayList(LineString){};
        errdefer {
            for (ls_list.items) |*ls| {
                ls.deinit(gpa);
            }
            ls_list.deinit(gpa);
        }
        try ls_list.appendSlice(gpa, linestrings);
        return .{ .linestrings = ls_list };
    }

    pub fn deinit(self: *MultiLineString, gpa: std.mem.Allocator) void {
        for (self.linestrings.items) |*ls| {
            ls.deinit(gpa);
        }
        self.linestrings.deinit(gpa);
    }

    pub fn getNumGeometries(self: MultiLineString) usize {
        return self.linestrings.items.len;
    }

    /// Get a linestring by index
    pub fn getGeometry(self: MultiLineString, index: usize) !LineString {
        if (index >= self.linestrings.items.len) return error.IndexOutOfBounds;
        return self.linestrings.items[index];
    }

    /// Convert to GEOS geometry (allocates - caller must destroy with GEOSGeom_destroy_r)
    pub fn toGeosGeometry(self: MultiLineString, ctx: *Context) !*c.GEOSGeometry {
        const geoms = try std.heap.page_allocator.alloc(*c.GEOSGeometry, self.linestrings.items.len);
        defer std.heap.page_allocator.free(geoms);

        for (self.linestrings.items, 0..) |ls, i| {
            geoms[i] = try ls.toGeosGeometry(ctx);
        }

        return try createCollection(ctx, std.heap.page_allocator, geoms, c.GEOS_MULTILINESTRING);
    }

    pub fn asGeometry(self: MultiLineString) Geometry {
        return .{ .multilinestring = self };
    }

    /// Clone this multilinestring (deep copy of all linestrings and their coordinates)
    pub fn clone(self: MultiLineString, gpa: std.mem.Allocator) !MultiLineString {
        var linestrings_clone = try std.ArrayList(LineString).initCapacity(gpa, self.linestrings.items.len);
        errdefer {
            for (linestrings_clone.items) |*ls| {
                ls.deinit(gpa);
            }
            linestrings_clone.deinit(gpa);
        }

        for (self.linestrings.items) |ls| {
            const ls_clone = try ls.clone(gpa);
            linestrings_clone.appendAssumeCapacity(ls_clone);
        }

        return .{
            .linestrings = linestrings_clone,
        };
    }
};

/// MultiPolygon geometry
pub const MultiPolygon = struct {
    polygons: std.ArrayList(Polygon),

    /// Create a multipolygon from polygons
    pub fn init(gpa: std.mem.Allocator, polygons: []const Polygon) !MultiPolygon {
        var poly_list = std.ArrayList(Polygon).empty;
        errdefer {
            for (poly_list.items) |*poly| {
                poly.deinit(gpa);
            }
            poly_list.deinit(gpa);
        }
        for (polygons) |polygon| {
            try poly_list.append(gpa, try polygon.clone(gpa));
        }
        return .{ .polygons = poly_list };
    }

    pub fn deinit(self: *MultiPolygon, gpa: std.mem.Allocator) void {
        for (self.polygons.items) |*poly|
            poly.deinit(gpa);

        self.polygons.deinit(gpa);
    }

    pub inline fn getBounds(self: MultiPolygon) BoundingBox {
        var min_x = std.math.floatMax(f64);
        var min_y = std.math.floatMax(f64);
        var max_x = std.math.floatMin(f64);
        var max_y = std.math.floatMin(f64);

        for (self.polygons.items) |poly| {
            for (poly.exterior) |coord| {
                min_x = @min(min_x, coord.x);
                min_y = @min(min_y, coord.y);
                max_x = @max(max_x, coord.x);
                max_y = @max(max_y, coord.y);
            }
        }
        return BoundingBox{
            .min_x = min_x,
            .min_y = min_y,
            .max_x = max_x,
            .max_y = max_y,
        };
    }

    pub fn getNumGeometries(self: MultiPolygon) usize {
        return self.polygons.items.len;
    }

    /// Get a polygon by index
    pub fn getGeometry(self: MultiPolygon, index: usize) !Polygon {
        if (index >= self.polygons.items.len) return error.IndexOutOfBounds;
        return self.polygons.items[index];
    }

    /// Convert to GEOS geometry (allocates - caller must destroy with GEOSGeom_destroy_r)
    pub fn toGeosGeometry(self: MultiPolygon, ctx: *Context) !*c.GEOSGeometry {
        const geoms = try std.heap.page_allocator.alloc(*c.GEOSGeometry, self.polygons.items.len);
        defer std.heap.page_allocator.free(geoms);

        for (self.polygons.items, 0..) |poly, i| {
            geoms[i] = try poly.toGeosGeometry(ctx);
        }

        return try createCollection(ctx, std.heap.page_allocator, geoms, c.GEOS_MULTIPOLYGON);
    }

    pub fn asGeometry(self: MultiPolygon) Geometry {
        return .{ .multipolygon = self };
    }

    /// Clone this multipolygon (deep copy of all polygons, their exteriors and holes)
    pub fn clone(self: MultiPolygon, gpa: std.mem.Allocator) !MultiPolygon {
        var polygons_clone = try std.ArrayList(Polygon).initCapacity(gpa, self.polygons.items.len);
        errdefer {
            for (polygons_clone.items) |*poly| {
                poly.deinit(gpa);
            }
            polygons_clone.deinit(gpa);
        }

        for (self.polygons.items) |poly| {
            const poly_clone = try poly.clone(gpa);
            polygons_clone.appendAssumeCapacity(poly_clone);
        }

        return .{
            .polygons = polygons_clone,
        };
    }
};

/// GeometryCollection
pub const GeometryCollection = struct {
    geometries: std.ArrayList(Geometry),
    alloc: std.mem.Allocator,

    /// Create a geometry collection
    pub fn init(gpa: std.mem.Allocator, geometries: []const Geometry) !GeometryCollection {
        var geom_list = std.ArrayList(Geometry){};
        errdefer {
            for (geom_list.items) |*geom| {
                geom.deinit(gpa);
            }
            geom_list.deinit(gpa);
        }
        try geom_list.appendSlice(gpa, geometries);
        return .{ .geometries = geom_list, .alloc = gpa };
    }

    pub fn deinit(self: *GeometryCollection, gpa: std.mem.Allocator) void {
        for (self.geometries.items) |*geom| {
            geom.deinit(gpa);
        }
        self.geometries.deinit(gpa);
    }

    pub fn getNumGeometries(self: GeometryCollection) usize {
        return self.geometries.items.len;
    }

    /// Get a geometry by index
    pub fn getGeometry(self: GeometryCollection, index: usize) !Geometry {
        if (index >= self.geometries.items.len) return error.IndexOutOfBounds;
        return self.geometries.items[index];
    }

    /// Convert to GEOS geometry (allocates - caller must destroy with GEOSGeom_destroy_r)
    pub fn toGeosGeometry(self: GeometryCollection, ctx: *Context) (error{ OutOfMemory, PointCreateFailed, LineStringCreateFailed, LinearRingCreateFailed, PolygonCreateFailed, CollectionCreateFailed, CloneFailed, GetCoordSeqFailed, InvalidPolygonCreated, GetExteriorRingFailed, GetNumInteriorRingsFailed, GetInteriorRingFailed, GetNumGeometriesFailed, GetGeometryFailed, CoordSeqCreateFailed, InvalidPointCreated })!*c.GEOSGeometry {
        const geoms = try std.heap.page_allocator.alloc(*c.GEOSGeometry, self.geometries.items.len);
        defer std.heap.page_allocator.free(geoms);

        for (self.geometries.items, 0..) |g, i| {
            geoms[i] = try g.toGeosGeometry(ctx);
        }

        return try createCollection(ctx, std.heap.page_allocator, geoms, c.GEOS_GEOMETRYCOLLECTION);
    }

    pub fn asGeometry(self: GeometryCollection) Geometry {
        return .{ .collection = self };
    }

    /// Clone this geometry collection (deep copy of all contained geometries)
    pub fn clone(self: GeometryCollection, gpa: std.mem.Allocator) !GeometryCollection {
        var geometries_clone = try std.ArrayList(Geometry).initCapacity(gpa, self.geometries.items.len);
        errdefer {
            for (geometries_clone.items) |*geom| {
                geom.deinit(gpa);
            }
            geometries_clone.deinit(gpa);
        }

        for (self.geometries.items) |geom| {
            const geom_clone = try geom.clone(gpa);
            geometries_clone.appendAssumeCapacity(geom_clone);
        }

        return .{
            .alloc = gpa,
            .geometries = geometries_clone,
        };
    }
};

/// Base geometry interface - all geometry types can be used as this
pub const Geometry = union(enum) {
    point: Point,
    linestring: LineString,
    linearring: LinearRing,
    polygon: Polygon,
    multipoint: MultiPoint,
    multilinestring: MultiLineString,
    multipolygon: MultiPolygon,
    collection: GeometryCollection,

    /// Convert to GEOS geometry (allocates - caller must destroy with GEOSGeom_destroy_r)
    pub fn toGeosGeometry(self: Geometry, ctx: *Context) !*c.GEOSGeometry {
        return switch (self) {
            inline else => |*geom| try geom.toGeosGeometry(ctx),
        };
    }

    pub fn deinit(self: *Geometry, gpa: std.mem.Allocator) void {
        switch (self.*) {
            .point => {}, // Points don't allocate
            inline else => |*geom| geom.deinit(gpa),
        }
    }

    /// Clone this geometry (deep copy of the data)
    pub fn clone(self: Geometry, allocator: std.mem.Allocator) error{OutOfMemory}!Geometry {
        return switch (self) {
            .point => |p| .{ .point = p }, // Points are copy-by-value
            inline else => |val, tag| @unionInit(Geometry, @tagName(tag), try val.clone(allocator)),
        };
    }

    pub fn isEmpty(self: Geometry) bool {
        return switch (self) {
            .point => false, // Points with coordinates are never empty
            .linestring => |ls| ls.coords.len == 0,
            .linearring => |lr| lr.coords.len == 0,
            .polygon => |poly| poly.exterior.len == 0,
            .multipoint => |mp| mp.points.items.len == 0,
            .multilinestring => |mls| mls.linestrings.items.len == 0,
            .multipolygon => |mpoly| mpoly.polygons.items.len == 0,
            .collection => |coll| coll.geometries.items.len == 0,
        };
    }

    pub fn isValid(self: Geometry) !bool {
        const ctx = try getDefaultContext();
        const geom = self.toGeosGeometry(ctx) catch |err| {
            switch (err) {
                error.InvalidPolygonCreated, error.LinearRingCreateFailed, error.PolygonCreateFailed => return false,
                else => return err,
            }
        };
        defer c.GEOSGeom_destroy_r(ctx.handle, geom);
        return c.GEOSisValid_r(ctx.handle, geom) == 1;
    }

    pub fn isSimple(self: Geometry) !bool {
        const ctx = try getDefaultContext();
        const geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, geom);
        return c.GEOSisSimple_r(ctx.handle, geom) == 1;
    }

    pub fn area(self: Geometry) !f64 {
        switch (self) {
            .point, .linestring, .linearring => return error.InvalidGeometryTypeForArea,
            else => {},
        }

        const ctx = try getDefaultContext();
        const geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, geom);

        // check if the geometry is valid
        if (c.GEOSisValid_r(ctx.handle, geom) == 0) {
            return error.InvalidGeometry;
        }

        var result: f64 = undefined;
        if (c.GEOSArea_r(ctx.handle, geom, &result) == 0) {
            return error.AreaCalculationFailed;
        }

        return result;
    }

    pub fn length(self: Geometry) !f64 {
        const ctx = try getDefaultContext();
        const geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, geom);

        var result: f64 = undefined;
        if (c.GEOSLength_r(ctx.handle, geom, &result) == 0) {
            return error.LengthCalculationFailed;
        }
        return result;
    }

    pub fn distance(self: Geometry, other: Geometry) !f64 {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        var result: f64 = undefined;
        if (c.GEOSDistance_r(ctx.handle, self_geom, other_geom, &result) == 0) {
            return error.DistanceCalculationFailed;
        }
        return result;
    }

    /// Check if distance between two geometries is within threshold
    /// Returns true if distance <= threshold, false otherwise
    pub fn distanceWithin(self: Geometry, other: Geometry, threshold: f64) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        const result = c.GEOSDistanceWithin_r(ctx.handle, self_geom, other_geom, threshold);
        if (result == 2) return error.DistanceWithinFailed;
        return result == 1;
    }

    /// Calculate the Hausdorff distance between two geometries
    /// The Hausdorff distance is the largest minimum distance between any point in one geometry
    /// and the closest point in the other geometry
    pub fn hausdorffDistance(self: Geometry, other: Geometry) !f64 {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        var result: f64 = undefined;
        if (c.GEOSHausdorffDistance_r(ctx.handle, self_geom, other_geom, &result) == 0) {
            return error.HausdorffDistanceCalculationFailed;
        }
        return result;
    }

    /// Find the two nearest points between two geometries
    /// Returns exactly 2 coordinates: [point on first, point on second]
    /// (allocates new geometry - caller owns)
    pub fn nearestPoints(first: Geometry, second: Geometry) ![2]Coordinate {
        const ctx = try getDefaultContext();
        const self_geom = try first.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try second.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        const coord_seq = c.GEOSNearestPoints_r(ctx.handle, self_geom, other_geom);
        if (coord_seq == null) return error.NearestPointsFailed;
        defer c.GEOSCoordSeq_destroy_r(ctx.handle, coord_seq);

        // Extract the two coordinates from the sequence
        var size: u32 = undefined;
        if (c.GEOSCoordSeq_getSize_r(ctx.handle, coord_seq, &size) == 0) {
            return error.CoordSeqGetSizeFailed;
        }
        assert(size == 2); // Always returns exactly 2 points

        var coords = [2]Coordinate{ undefined, undefined };
        for (0..2) |i| {
            var x: f64 = undefined;
            var y: f64 = undefined;
            if (c.GEOSCoordSeq_getXY_r(ctx.handle, coord_seq, @intCast(i), &x, &y) == 0) {
                return error.CoordSeqGetXYFailed;
            }
            coords[i] = .{ .x = x, .y = y };
        }

        return coords;
    }

    pub fn intersects(self: Geometry, other: Geometry) !bool {
        const ctx = try getDefaultContext();

        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        return c.GEOSIntersects_r(ctx.handle, self_geom, other_geom) == 1;
    }

    pub fn contains(self: Geometry, other: Geometry) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);
        return c.GEOSContains_r(ctx.handle, self_geom, other_geom) == 1;
    }

    pub fn within(self: Geometry, other: Geometry) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);
        return c.GEOSWithin_r(ctx.handle, self_geom, other_geom) == 1;
    }

    pub fn touches(self: Geometry, other: Geometry) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);
        return c.GEOSTouches_r(ctx.handle, self_geom, other_geom) == 1;
    }

    pub fn overlaps(self: Geometry, other: Geometry) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);
        return c.GEOSOverlaps_r(ctx.handle, self_geom, other_geom) == 1;
    }

    pub fn crosses(self: Geometry, other: Geometry) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);
        return c.GEOSCrosses_r(ctx.handle, self_geom, other_geom) == 1;
    }

    pub fn disjoint(self: Geometry, other: Geometry) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);
        return c.GEOSDisjoint_r(ctx.handle, self_geom, other_geom) == 1;
    }

    pub fn equals(self: Geometry, other: Geometry) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);
        return c.GEOSEquals_r(ctx.handle, self_geom, other_geom) == 1;
    }

    pub fn equalsExact(self: Geometry, other: Geometry, tolerance: f64) !bool {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);
        return c.GEOSEqualsExact_r(ctx.handle, self_geom, other_geom, tolerance) == 1;
    }

    /// Create a buffer around this geometry (allocates new geometry - caller owns)
    pub fn buffer(self: Geometry, allocator: std.mem.Allocator, dist: f64, quadsegs: i32) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSBuffer_r(ctx.handle, self_geom, dist, quadsegs) orelse return error.BufferFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result);
    }

    /// End cap styles for buffer operations
    pub const BufferCapStyle = enum(c_int) {
        round = 1,
        flat = 2,
        square = 3,
    };

    /// Join styles for buffer operations
    pub const BufferJoinStyle = enum(c_int) {
        round = 1,
        mitre = 2,
        bevel = 3,
    };

    /// Buffer side selection
    pub const BufferSide = enum(c_int) {
        both = 0,
        left = 1,
        right = 2,
    };

    /// Parameters for advanced buffer operations
    pub const BufferParams = struct {
        quadsegs: i32 = 8,
        end_cap_style: BufferCapStyle = .round,
        join_style: BufferJoinStyle = .round,
        mitre_limit: f64 = 5.0,
        side: BufferSide = .both,
    };

    /// Create a buffer with advanced options (allocates new geometry - caller owns)
    /// Supports single-sided buffers (left/right), different cap styles, and join styles
    pub fn bufferWithParams(self: Geometry, allocator: std.mem.Allocator, dist: f64, params: BufferParams) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const buffer_params = c.GEOSBufferParams_create_r(ctx.handle) orelse return error.BufferParamsCreateFailed;
        defer c.GEOSBufferParams_destroy_r(ctx.handle, buffer_params);

        if (c.GEOSBufferParams_setEndCapStyle_r(ctx.handle, buffer_params, @intFromEnum(params.end_cap_style)) == 0) {
            return error.BufferParamsSetFailed;
        }
        if (c.GEOSBufferParams_setJoinStyle_r(ctx.handle, buffer_params, @intFromEnum(params.join_style)) == 0) {
            return error.BufferParamsSetFailed;
        }
        if (c.GEOSBufferParams_setMitreLimit_r(ctx.handle, buffer_params, params.mitre_limit) == 0) {
            return error.BufferParamsSetFailed;
        }
        if (c.GEOSBufferParams_setQuadrantSegments_r(ctx.handle, buffer_params, params.quadsegs) == 0) {
            return error.BufferParamsSetFailed;
        }
        if (c.GEOSBufferParams_setSingleSided_r(ctx.handle, buffer_params, @intFromBool(params.side != .both)) == 0) {
            return error.BufferParamsSetFailed;
        }

        const result = c.GEOSBufferWithParams_r(ctx.handle, self_geom, buffer_params, dist) orelse return error.BufferFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result);
    }

    /// Snap geometry to target geometry within tolerance (allocates new geometry - caller owns)
    /// Moves vertices of this geometry to lie on the target geometry boundary within tolerance
    pub fn snap(self: Geometry, allocator: std.mem.Allocator, target: Geometry, tolerance: f64) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const target_geom = try target.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, target_geom);

        const result = c.GEOSSnap_r(ctx.handle, self_geom, target_geom, tolerance) orelse return error.SnapFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result);
    }

    /// Compute convex hull (allocates new geometry - caller owns)
    pub fn convexHull(self: Geometry, allocator: std.mem.Allocator) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSConvexHull_r(ctx.handle, self_geom);
        if (result == null) return error.ConvexHullFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Compute minimum rotated rectangle / oriented bounding box (allocates new geometry - caller owns)
    pub fn minimumRotatedRectangle(self: Geometry, allocator: std.mem.Allocator) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSMinimumRotatedRectangle_r(ctx.handle, self_geom) orelse return error.MinimumRotatedRectangleFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        // assert there are no holes
        assert(c.GEOSGetNumInteriorRings_r(ctx.handle, result) == 0);

        return try fromGEOSGeom(allocator, ctx, result);
    }

    /// Compute centroid (allocates new geometry - caller owns)
    pub fn centroid(self: Geometry) !Point {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSGetCentroid_r(ctx.handle, self_geom) orelse return error.CentroidFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        if (c.GEOSisEmpty_r(ctx.handle, result) == 1)
            return error.CentroidEmpty;

        // Extract coordinate from centroid point
        var x: f64 = undefined;
        var y: f64 = undefined;
        assert(c.GEOSGeomGetX_r(ctx.handle, result, &x) != 0);
        assert(c.GEOSGeomGetY_r(ctx.handle, result, &y) != 0);

        return Point{ .coord = .{ .x = x, .y = y } };
    }

    /// Compute envelope/bounding box (allocates new geometry - caller owns)
    pub fn envelope(self: Geometry, allocator: std.mem.Allocator) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSEnvelope_r(ctx.handle, self_geom);
        if (result == null) return error.EnvelopeFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Compute boundary (allocates new geometry - caller owns)
    pub fn boundary(self: Geometry, allocator: std.mem.Allocator) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSBoundary_r(ctx.handle, self_geom);
        if (result == null) return error.BoundaryFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Simplify geometry (allocates new geometry - caller owns)
    /// HINT: using tolerance of 0.0 will remove all unnecessary coordinates
    pub fn simplify(self: Geometry, allocator: std.mem.Allocator, tolerance: f64) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSSimplify_r(ctx.handle, self_geom, tolerance);
        if (result == null) return error.SimplifyFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Simplify geometry preserving topology (allocates new geometry - caller owns)
    pub fn simplifyPreserveTopology(self: Geometry, allocator: std.mem.Allocator, tolerance: f64) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSTopologyPreserveSimplify_r(ctx.handle, self_geom, tolerance);
        if (result == null) return error.SimplifyFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Compute intersection with another geometry (allocates new geometry - caller owns)
    pub fn intersection(self: Geometry, allocator: std.mem.Allocator, other: Geometry) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        const result = c.GEOSIntersection_r(ctx.handle, self_geom, other_geom);
        if (result == null) return error.IntersectionFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Compute union with another geometry (allocates new geometry - caller owns)
    pub fn unionGeom(self: Geometry, allocator: std.mem.Allocator, other: Geometry) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        const result = c.GEOSUnion_r(ctx.handle, self_geom, other_geom);
        if (result == null) return error.UnionFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Compute difference with another geometry (allocates new geometry - caller owns)
    pub fn difference(self: Geometry, allocator: std.mem.Allocator, other: Geometry) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        const result = c.GEOSDifference_r(ctx.handle, self_geom, other_geom);
        if (result == null) return error.DifferenceFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Compute symmetric difference with another geometry (allocates new geometry - caller owns)
    pub fn symDifference(self: Geometry, allocator: std.mem.Allocator, other: Geometry) !Geometry {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);
        const other_geom = try other.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, other_geom);

        const result = c.GEOSSymDifference_r(ctx.handle, self_geom, other_geom);
        if (result == null) return error.SymDifferenceFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Batch union of multiple geometries using GEOSUnaryUnion
    /// Input must be a GeometryCollection or MultiPolygon, returns error otherwise
    /// More efficient than repeated pairwise unions (allocates new geometry - caller owns)
    pub fn batchUnion(self: Geometry, allocator: std.mem.Allocator) !Geometry {
        // var timer = try std.time.Timer.start();
        // defer std.debug.print("batchUnion took: {D}\n", .{timer.lap()});

        switch (self) {
            .collection, .multipolygon => {},
            else => {
                std.log.err("Invalid geometry type for batch union: {t}", .{self});
                return error.InvalidGeometryTypeForBatchUnion;
            },
        }

        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSUnaryUnion_r(ctx.handle, self_geom);
        if (result == null) return error.BatchUnionFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Union of a coverage (non-overlapping adjacent polygons) - GEOS 3.12+
    /// More efficient than batchUnion for non-overlapping grid cells
    /// Input must be a GeometryCollection or MultiPolygon of non-overlapping adjacent polygons
    /// Validates that input forms a valid coverage (properly noded, no overlaps)
    /// (allocates new geometry - caller owns)
    pub fn coverageUnion(self: Geometry, allocator: std.mem.Allocator) !Geometry {
        switch (self) {
            .collection, .multipolygon => {},
            else => {
                std.log.err("Invalid geometry type for coverage union: {t}", .{self});
                return error.InvalidGeometryTypeForCoverageUnion;
            },
        }

        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSCoverageUnion_r(ctx.handle, self_geom);
        if (result == null) return error.CoverageUnionFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        return try fromGEOSGeom(allocator, ctx, result.?);
    }

    /// Convert to WKT (Well-Known Text) format (allocates memory for result)
    pub fn toWKT(self: Geometry, allocator: std.mem.Allocator) ![]u8 {
        const ctx = try getDefaultContext();
        const geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, geom);

        const writer = c.GEOSWKTWriter_create_r(ctx.handle) orelse return error.WKTWriterCreateFailed;
        defer c.GEOSWKTWriter_destroy_r(ctx.handle, writer);

        const wkt_cstr = c.GEOSWKTWriter_write_r(ctx.handle, writer, geom);
        if (wkt_cstr == null) return error.WKTConversionFailed;
        defer c.GEOSFree_r(ctx.handle, wkt_cstr);

        const wkt_slice = std.mem.span(wkt_cstr);
        return allocator.dupe(u8, wkt_slice);
    }

    /// Convert to WKB (Well-Known Binary) format (allocates memory for result)
    pub fn toWKB(self: Geometry, allocator: std.mem.Allocator) ![]u8 {
        const ctx = try getDefaultContext();
        const geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, geom);

        const writer = c.GEOSWKBWriter_create_r(ctx.handle) orelse return error.WKBWriterCreateFailed;
        defer c.GEOSWKBWriter_destroy_r(ctx.handle, writer);

        var size: usize = undefined;
        const wkb_ptr = c.GEOSWKBWriter_write_r(ctx.handle, writer, geom, &size);
        if (wkb_ptr == null) return error.WKBConversionFailed;
        defer c.GEOSFree_r(ctx.handle, wkb_ptr);

        const wkb_slice = try allocator.alloc(u8, size);
        @memcpy(wkb_slice, wkb_ptr[0..size]);
        return wkb_slice;
    }

    /// Parse WKT string into geometry using default context (allocates temporary memory and result geometry)
    pub fn fromWKT(allocator: std.mem.Allocator, wkt: []const u8) !Geometry {
        const ctx = try getDefaultContext();
        return fromWKTWithContext(allocator, ctx, wkt);
    }

    /// Parse WKT string into geometry using explicit context (allocates temporary memory and result geometry)
    pub fn fromWKTWithContext(allocator: std.mem.Allocator, ctx: *Context, wkt: []const u8) !Geometry {
        const reader = c.GEOSWKTReader_create_r(ctx.handle) orelse return error.WKTReaderCreateFailed;
        defer c.GEOSWKTReader_destroy_r(ctx.handle, reader);

        const wkt_cstr = try allocator.dupeZ(u8, wkt);
        defer allocator.free(wkt_cstr);

        const geom = c.GEOSWKTReader_read_r(ctx.handle, reader, wkt_cstr.ptr);
        if (geom == null) return error.WKTParsingFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, geom);

        return try fromGEOSGeom(allocator, ctx, geom.?);
    }

    /// Parse WKB bytes into geometry using default context (allocates result geometry)
    pub fn fromWKB(allocator: std.mem.Allocator, wkb: []const u8) !Geometry {
        const ctx = try getDefaultContext();
        return fromWKBWithContext(allocator, ctx, wkb);
    }

    /// Parse WKB bytes into geometry using explicit context (allocates result geometry)
    pub fn fromWKBWithContext(allocator: std.mem.Allocator, ctx: *Context, wkb: []const u8) !Geometry {
        const reader = c.GEOSWKBReader_create_r(ctx.handle) orelse return error.WKBReaderCreateFailed;
        defer c.GEOSWKBReader_destroy_r(ctx.handle, reader);

        const geom = c.GEOSWKBReader_read_r(ctx.handle, reader, wkb.ptr, wkb.len);
        if (geom == null) return error.WKBParsingFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, geom);

        return try fromGEOSGeom(allocator, ctx, geom.?);
    }

    /// Get the dimension of the geometry (0=point, 1=line, 2=area)
    pub fn getDimension(self: Geometry) i32 {
        return switch (self) {
            .point => 0,
            .linestring, .linearring, .multilinestring => 1,
            .polygon, .multipolygon => 2,
            .multipoint, .collection => 0, // Mixed/undefined
        };
    }

    /// Get the coordinate dimension (2 or 3)
    pub fn getCoordinateDimension(_: Geometry) i32 {
        // For now, we only support 2D
        return 2;
    }

    /// Get SRID (Spatial Reference System Identifier) - not yet supported
    pub fn getSRID(_: Geometry) i32 {
        return 0; // Default SRID
    }

    /// Set SRID - not yet supported
    pub fn setSRID(_: Geometry, _: i32) void {
        // Not yet supported in Zig-native storage
    }

    pub fn getBounds(self: Geometry) !BoundingBox {
        switch (self) {
            .point => |p| return p.getBounds(),
            .multipoint => |mp| return mp.getBounds(),
            .polygon => |poly| return poly.getBounds(),
            .multipolygon => |mltply| return mltply.getBounds(),
            inline else => {
                const ctx = try getDefaultContext();
                const self_geom = try self.toGeosGeometry(ctx);
                defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

                var min_x: f64 = undefined;
                var min_y: f64 = undefined;
                var max_x: f64 = undefined;
                var max_y: f64 = undefined;

                if (c.GEOSGeom_getXMin_r(ctx.handle, self_geom, &min_x) == 0) return error.GeosError;
                if (c.GEOSGeom_getXMax_r(ctx.handle, self_geom, &max_x) == 0) return error.GeosError;
                if (c.GEOSGeom_getYMin_r(ctx.handle, self_geom, &min_y) == 0) return error.GeosError;
                if (c.GEOSGeom_getYMax_r(ctx.handle, self_geom, &max_y) == 0) return error.GeosError;

                return BoundingBox{
                    .min_x = min_x,
                    .min_y = min_y,
                    .max_x = max_x,
                    .max_y = max_y,
                };
            },
        }
    }

    /// Compute constrained Delaunay triangulation of the polygon (allocates new geometry - caller owns)
    /// Returns a GeometryCollection of Polygon triangles
    pub fn constrainedDelaunayTriangulation(self: Geometry, allocator: std.mem.Allocator) !GeometryCollection {
        const ctx = try getDefaultContext();
        const self_geom = try self.toGeosGeometry(ctx);
        defer c.GEOSGeom_destroy_r(ctx.handle, self_geom);

        const result = c.GEOSConstrainedDelaunayTriangulation_r(ctx.handle, self_geom);
        if (result == null) return error.TriangulationFailed;
        defer c.GEOSGeom_destroy_r(ctx.handle, result);

        // The result should be a GeometryCollection, so we need to convert it
        const geom = try fromGEOSGeom(allocator, ctx, result.?);

        // Return as GeometryCollection - GEOS always returns GeometryCollection
        assert(geom == .collection);
        return geom.collection;
    }
};

/// Prepared geometry for efficient repeated spatial operations
/// Creates an internal spatial index for much faster intersection/containment tests
/// Best used when testing many geometries against the same base geometry
/// Must call deinit() when done
pub const PreparedGeometry = struct {
    handle: *const c.GEOSPreparedGeometry,
    ctx: *Context,
    source_geom: *c.GEOSGeometry,

    /// Create a prepared geometry from a regular geometry
    /// The prepared geometry takes ownership of the GEOS geometry handle
    pub fn init(geom: Geometry) !PreparedGeometry {
        const ctx = try getDefaultContext();
        const geom_handle = try geom.toGeosGeometry(ctx);

        const prep = c.GEOSPrepare_r(ctx.handle, geom_handle) orelse return error.PrepareFailed;

        return PreparedGeometry{
            .handle = prep,
            .ctx = ctx,
            .source_geom = geom_handle,
        };
    }

    /// Free the prepared geometry and its source geometry
    pub fn deinit(self: PreparedGeometry) void {
        c.GEOSPreparedGeom_destroy_r(self.ctx.handle, self.handle);
        c.GEOSGeom_destroy_r(self.ctx.handle, self.source_geom);
    }

    /// Test if this prepared geometry intersects another geometry
    /// Much faster than Geometry.intersects() when called repeatedly
    pub fn intersects(self: PreparedGeometry, other: Geometry) !bool {
        const other_geom = try other.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, other_geom);

        const result = c.GEOSPreparedIntersects_r(self.ctx.handle, self.handle, other_geom);
        if (result == 2) return error.IntersectsFailed;
        return result == 1;
    }

    /// Test if this prepared geometry touches another geometry
    /// Much faster than Geometry.touches() when called repeatedly
    pub fn touches(self: PreparedGeometry, other: Geometry) !bool {
        const other_geom = try other.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, other_geom);

        const result = c.GEOSPreparedTouches_r(self.ctx.handle, self.handle, other_geom);
        if (result == 2) return error.TouchesFailed;
        return result == 1;
    }

    /// Test if this prepared geometry contains another geometry
    pub fn contains(self: PreparedGeometry, other: Geometry) !bool {
        const other_geom = try other.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, other_geom);

        const result = c.GEOSPreparedContains_r(self.ctx.handle, self.handle, other_geom);
        if (result == 2) return error.ContainsFailed;
        return result == 1;
    }

    /// Calculate the distance from this prepared geometry to another geometry
    /// Much faster than Geometry.distance() when called repeatedly with the same prepared geometry
    pub fn distance(self: PreparedGeometry, other: Geometry) !f64 {
        const other_geom = try other.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, other_geom);

        var dist: f64 = undefined;
        const result = c.GEOSPreparedDistance_r(self.ctx.handle, self.handle, other_geom, &dist);
        if (result == 0) return error.DistanceFailed;
        return dist;
    }

    /// Test if the distance between this prepared geometry and another is within a threshold
    /// Returns true if distance <= threshold
    /// More efficient than calculating actual distance when you only need to check a threshold
    pub fn distanceWithin(self: PreparedGeometry, other: Geometry, threshold: f64) !bool {
        const other_geom = try other.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, other_geom);

        const result = c.GEOSPreparedDistanceWithin_r(self.ctx.handle, self.handle, other_geom, threshold);
        if (result == 2) return error.DistanceWithinFailed;
        return result == 1;
    }

    /// !intersects(other)
    pub fn disjoint(self: PreparedGeometry, other: Geometry) !bool {
        const other_geom = try other.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, other_geom);

        const result = c.GEOSPreparedDisjoint_r(self.ctx.handle, self.handle, other_geom);
        if (result == 2) return error.DisjointFailed;
        return result == 1;
    }

    /// Get the underlying geometry as a Geometry object
    /// The geometry is cloned, so caller must call .deinit() on the returned geometry
    pub fn getGeometry(self: PreparedGeometry, allocator: std.mem.Allocator) !Geometry {
        const cloned = c.GEOSGeom_clone_r(self.ctx.handle, self.source_geom) orelse return error.CloneFailed;
        return fromGEOSGeom(allocator, self.ctx, cloned);
    }
};

pub fn fromGeosGeometry(gpa: std.mem.Allocator, geom: *c.GEOSGeometry) !Geometry {
    const ctx = try getDefaultContext();
    return fromGEOSGeom(gpa, ctx, geom);
}

/// Helper to convert a GEOS geometry into Zig-native geometry types
/// Extracts all data from GEOS and stores it in our own structures
fn fromGEOSGeom(allocator: std.mem.Allocator, ctx: *Context, geom: *c.GEOSGeometry) !Geometry {
    const type_id = c.GEOSGeomTypeId_r(ctx.handle, geom);

    return switch (type_id) {
        c.GEOS_POINT => blk: {
            var x: f64 = undefined;
            var y: f64 = undefined;
            _ = c.GEOSGeomGetX_r(ctx.handle, geom, &x);
            _ = c.GEOSGeomGetY_r(ctx.handle, geom, &y);
            break :blk .{ .point = Point{ .coord = .{ .x = x, .y = y } } };
        },
        c.GEOS_LINESTRING => blk: {
            const seq = c.GEOSGeom_getCoordSeq_r(ctx.handle, geom) orelse return error.GetCoordSeqFailed;
            const coords = try readCoordSeq(ctx, seq, allocator);
            defer allocator.free(coords);

            const ls = try LineString.init(allocator, coords);
            break :blk .{ .linestring = ls };
        },
        c.GEOS_LINEARRING => blk: {
            const seq = c.GEOSGeom_getCoordSeq_r(ctx.handle, geom) orelse return error.GetCoordSeqFailed;
            const coords = try readCoordSeq(ctx, seq, allocator);
            defer allocator.free(coords);

            const lr = try LinearRing.init(allocator, coords);
            break :blk .{ .linearring = lr };
        },
        c.GEOS_POLYGON => blk: {
            // Extract exterior ring
            const ext_ring = c.GEOSGetExteriorRing_r(ctx.handle, geom) orelse return error.GetExteriorRingFailed;
            const ext_seq = c.GEOSGeom_getCoordSeq_r(ctx.handle, ext_ring) orelse return error.GetCoordSeqFailed;
            const exterior = try readCoordSeq(ctx, ext_seq, allocator);
            defer allocator.free(exterior);

            // Extract holes
            const num_holes_signed = c.GEOSGetNumInteriorRings_r(ctx.handle, geom);
            if (num_holes_signed < 0) return error.GetNumInteriorRingsFailed;
            const num_holes: usize = @intCast(num_holes_signed);

            const holes = try allocator.alloc([]const Coordinate, num_holes);
            defer allocator.free(holes);
            defer for (holes) |hole| allocator.free(hole);

            for (0..num_holes) |i| {
                const hole_ring = c.GEOSGetInteriorRingN_r(ctx.handle, geom, @intCast(i)) orelse return error.GetInteriorRingFailed;
                const hole_seq = c.GEOSGeom_getCoordSeq_r(ctx.handle, hole_ring) orelse return error.GetCoordSeqFailed;
                holes[i] = try readCoordSeq(ctx, hole_seq, allocator);
            }

            const poly = try Polygon.init(allocator, exterior, holes);
            break :blk .{ .polygon = poly };
        },
        c.GEOS_MULTIPOINT => blk: {
            const num_geoms_signed = c.GEOSGetNumGeometries_r(ctx.handle, geom);
            if (num_geoms_signed < 0) return error.GetNumGeometriesFailed;
            const num_geoms: usize = @intCast(num_geoms_signed);

            const points = try allocator.alloc(Point, num_geoms);
            defer allocator.free(points);

            for (0..num_geoms) |i| {
                const pt_geom = c.GEOSGetGeometryN_r(ctx.handle, geom, @intCast(i)) orelse return error.GetGeometryFailed;
                var x: f64 = undefined;
                var y: f64 = undefined;
                _ = c.GEOSGeomGetX_r(ctx.handle, pt_geom, &x);
                _ = c.GEOSGeomGetY_r(ctx.handle, pt_geom, &y);
                points[i] = Point.init(.{ .x = x, .y = y });
            }

            const mp = try MultiPoint.init(allocator, points);
            break :blk .{ .multipoint = mp };
        },
        c.GEOS_MULTILINESTRING => blk: {
            const num_geoms_signed = c.GEOSGetNumGeometries_r(ctx.handle, geom);
            if (num_geoms_signed < 0) return error.GetNumGeometriesFailed;
            const num_geoms: usize = @intCast(num_geoms_signed);

            const linestrings = try allocator.alloc(LineString, num_geoms);
            defer allocator.free(linestrings);

            for (0..num_geoms) |i| {
                const ls_geom = c.GEOSGetGeometryN_r(ctx.handle, geom, @intCast(i)) orelse return error.GetGeometryFailed;
                const seq = c.GEOSGeom_getCoordSeq_r(ctx.handle, ls_geom) orelse return error.GetCoordSeqFailed;
                const coords = try readCoordSeq(ctx, seq, allocator);
                defer allocator.free(coords);
                linestrings[i] = try LineString.init(allocator, coords);
            }

            const mls = try MultiLineString.init(allocator, linestrings);
            break :blk .{ .multilinestring = mls };
        },
        c.GEOS_MULTIPOLYGON => blk: {
            const num_geoms_signed = c.GEOSGetNumGeometries_r(ctx.handle, geom);
            if (num_geoms_signed < 0) return error.GetNumGeometriesFailed;
            const num_geoms: usize = @intCast(num_geoms_signed);

            const polygons = try allocator.alloc(Polygon, num_geoms);
            defer allocator.free(polygons);

            for (0..num_geoms) |i| {
                const poly_geom = c.GEOSGetGeometryN_r(ctx.handle, geom, @intCast(i)) orelse return error.GetGeometryFailed;

                // Extract polygon data
                const ext_ring = c.GEOSGetExteriorRing_r(ctx.handle, poly_geom) orelse return error.GetExteriorRingFailed;
                const ext_seq = c.GEOSGeom_getCoordSeq_r(ctx.handle, ext_ring) orelse return error.GetCoordSeqFailed;
                const exterior = try readCoordSeq(ctx, ext_seq, allocator);
                defer allocator.free(exterior);

                const num_holes_signed = c.GEOSGetNumInteriorRings_r(ctx.handle, poly_geom);
                if (num_holes_signed < 0) return error.GetNumInteriorRingsFailed;
                const num_holes: usize = @intCast(num_holes_signed);

                const holes = try allocator.alloc([]const Coordinate, num_holes);
                defer allocator.free(holes);
                defer for (holes) |hole| allocator.free(hole);

                for (0..num_holes) |j| {
                    const hole_ring = c.GEOSGetInteriorRingN_r(ctx.handle, poly_geom, @intCast(j)) orelse return error.GetInteriorRingFailed;
                    const hole_seq = c.GEOSGeom_getCoordSeq_r(ctx.handle, hole_ring) orelse return error.GetCoordSeqFailed;
                    holes[j] = try readCoordSeq(ctx, hole_seq, allocator);
                }

                polygons[i] = try Polygon.init(allocator, exterior, holes);
            }

            const mpoly = try MultiPolygon.init(allocator, polygons);
            break :blk .{ .multipolygon = mpoly };
        },
        c.GEOS_GEOMETRYCOLLECTION => blk: {
            const num_geoms_signed = c.GEOSGetNumGeometries_r(ctx.handle, geom);
            if (num_geoms_signed < 0) return error.GetNumGeometriesFailed;
            const num_geoms: usize = @intCast(num_geoms_signed);

            const geometries = try allocator.alloc(Geometry, num_geoms);
            defer allocator.free(geometries);

            for (0..num_geoms) |i| {
                const sub_geom = c.GEOSGetGeometryN_r(ctx.handle, geom, @intCast(i)) orelse return error.GetGeometryFailed;
                geometries[i] = try fromGEOSGeom(allocator, ctx, @constCast(sub_geom));
            }

            const gc = try GeometryCollection.init(allocator, geometries);
            break :blk .{ .collection = gc };
        },
        else => unreachable, // GEOS only returns these 8 geometry types
    };
}

pub const STRTree = struct {
    ctx: *Context,
    tree: *c.GEOSSTRtree,

    /// normal node_capacity is between 5 and 10
    pub fn initCapacity(node_capacity: usize) !STRTree {
        const ctx = try getDefaultContext();
        const tree = c.GEOSSTRtree_create_r(ctx.handle, node_capacity) orelse
            return error.STRTreeCreateFailed;

        return STRTree{
            .ctx = ctx,
            .tree = tree,
        };
    }

    pub fn deinit(self: STRTree) void {
        c.GEOSSTRtree_destroy_r(self.ctx.handle, self.tree);
    }

    /// Finalize the tree structure after all insertions.
    /// *After calling this, no more items can be inserted.*
    /// Note: This is optional - queries will auto-build if needed.
    /// Returns error if build failed.
    pub fn finalize(self: STRTree) !void {
        const result = c.GEOSSTRtree_build_r(self.ctx.handle, self.tree);
        if (result != 1) {
            return error.STRTreeBuildFailed;
        }
    }

    /// Insert geometry and an optional payload pointer
    /// Note: This creates a GEOS geometry internally - caller should deinit geometry after batch inserts
    pub fn insert(self: STRTree, geom: Geometry, item: ?*anyopaque) !void {
        const geos_geom = try geom.toGeosGeometry(self.ctx);
        // Note: STRTree takes ownership - don't destroy here
        c.GEOSSTRtree_insert_r(self.ctx.handle, self.tree, geos_geom, item);
    }

    /// Remove a geometry/item pair
    pub fn remove(self: STRTree, geom: Geometry, item: ?*const anyopaque) !void {
        const geos_geom = try geom.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, geos_geom);
        c.GEOSSTRtree_remove_r(self.ctx.handle, self.tree, geos_geom, item);
    }

    /// Query the tree for geometries overlapping the given envelope or geometry.
    /// The callback is called with each item pointer (may be null if inserted that way).
    pub fn query(
        self: STRTree,
        geom: Geometry,
        comptime CallbackFn: fn (?*anyopaque, ?*anyopaque) callconv(.c) void,
        userdata: ?*anyopaque,
    ) !void {
        const geos_geom = try geom.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, geos_geom);
        c.GEOSSTRtree_query_r(self.ctx.handle, self.tree, geos_geom, CallbackFn, userdata);
    }

    /// Iterate over all items in the tree (full traversal)
    pub fn iterate(
        self: STRTree,
        comptime CallbackFn: fn (?*anyopaque, ?*anyopaque) callconv(.C) void,
        userdata: ?*anyopaque,
    ) void {
        c.GEOSSTRtree_iterate_r(self.ctx.handle, self.tree, CallbackFn, userdata);
    }

    /// Convenience helper for simple query use-cases
    pub fn querySimple(self: STRTree, geom: Geometry, callback: fn (?*anyopaque) void) !void {
        const geos_geom = try geom.toGeosGeometry(self.ctx);
        defer c.GEOSGeom_destroy_r(self.ctx.handle, geos_geom);

        const Wrapper = struct {
            pub fn adapter(item: ?*anyopaque, userdata: ?*anyopaque) callconv(.C) void {
                const cb: *const fn (?*anyopaque) void = @ptrCast(userdata.?);
                cb(item);
            }
        };
        c.GEOSSTRtree_query_r(
            self.ctx.handle,
            self.tree,
            geos_geom,
            Wrapper.adapter,
            @constCast(&callback),
        );
    }

    /// Query and return results as a typed ArrayList (allocates memory for results)
    /// Type T should be the type of items stored in the tree
    pub fn queryTyped(
        self: STRTree,
        allocator: std.mem.Allocator,
        geom: Geometry,
        comptime T: type,
    ) !std.ArrayList(*T) {
        var results = std.ArrayList(*T).init(allocator);
        errdefer results.deinit();

        const CallbackContext = struct {
            fn callback(item: ?*anyopaque, userdata: ?*anyopaque) callconv(.C) void {
                const list: *std.ArrayList(*T) = @ptrCast(@alignCast(userdata));
                if (item) |ptr| {
                    const typed: *T = @ptrCast(@alignCast(ptr));
                    list.append(typed) catch {};
                }
            }
        };

        try self.query(geom, CallbackContext.callback, &results);
        return results;
    }

    /// Query and return indices (allocates memory for results)
    /// Use this when you've inserted geometries with indices as items via @intToPtr
    /// This is useful for spatial indexing of array elements
    pub fn queryIndices(
        self: STRTree,
        allocator: std.mem.Allocator,
        geom: Geometry,
    ) ![]usize {
        var results = std.ArrayList(usize).empty;
        errdefer results.deinit(allocator);

        const QueryContext = struct {
            list: *std.ArrayList(usize),
            allocator: std.mem.Allocator,
            mem_error_occurred: bool,
        };

        var ctx = QueryContext{
            .list = &results,
            .allocator = allocator,
            .mem_error_occurred = false,
        };

        const CallbackContext = struct {
            fn callback(item: ?*anyopaque, userdata: ?*anyopaque) callconv(.c) void {
                const context: *QueryContext = @ptrCast(@alignCast(userdata));
                if (item) |ptr| {
                    // Convert back from 1-based to 0-based indexing
                    const index: usize = @intFromPtr(ptr) - 1;
                    context.list.append(context.allocator, index) catch {
                        context.mem_error_occurred = true;
                        return;
                    };
                }
            }
        };

        if (ctx.mem_error_occurred)
            return error.OutOfMemory;

        try self.query(geom, CallbackContext.callback, &ctx);
        return try results.toOwnedSlice(allocator);
    }

    /// Insert a geometry with an index as the item
    /// Use with queryIndices to get back the indices
    pub fn insertIndex(self: STRTree, geom: Geometry, index: usize) !void {
        const geos_geom = try geom.toGeosGeometry(self.ctx);
        // Store as 1-based to avoid null pointer (0 would be null)
        // Note: STRTree takes ownership - don't destroy here
        c.GEOSSTRtree_insert_r(self.ctx.handle, self.tree, geos_geom, @ptrFromInt(index + 1));
    }

    /// Batch insertion (just loops, but preallocates envelope geometries if needed)
    /// GEOS itself doesn't have a special bulk-load function, but this avoids
    /// unnecessary envelope creation per insert.
    pub fn batchInsert(self: STRTree, geometries: []const Geometry, items: []const ?*anyopaque) void {
        assert(geometries.len == items.len); // Caller must provide matching lengths

        // GEOSSTRtree_insert_r already does minimal envelope computation internally,
        // so there's no direct way to make it "faster", but you can reduce allocations
        // by reusing envelopes or precomputing them if you know your geometries are large.
        for (geometries, 0..) |g, i| {
            c.GEOSSTRtree_insert_r(self.ctx.handle, self.tree, g.getGeom(), items[i]);
        }
    }

    /// Clear all items — easiest done by destroying and recreating the tree
    pub fn clear(self: *STRTree, capacity: usize) !void {
        c.GEOSSTRtree_destroy_r(self.ctx.handle, self.tree);
        self.tree = c.GEOSSTRtree_create_r(self.ctx.handle, capacity) orelse
            return error.STRTreeCreateFailed;
    }
};

test "GEOS touches behavior after intersection" {
    try init();
    defer deinit();

    const gpa = std.testing.allocator;

    // Create a target rectangle
    var target = try Polygon.initFromExterior(gpa, &.{
        .{ .x = 0, .y = 0 },
        .{ .x = 100, .y = 0 },
        .{ .x = 100, .y = 100 },
        .{ .x = 0, .y = 100 },
        .{ .x = 0, .y = 0 },
    });
    defer target.deinit(gpa);

    var target_ring = try target.exteriorRing(gpa);
    defer target_ring.deinit(gpa);

    // Create a cell that partially overlaps the target boundary (like cells at grid edges)
    var cell = try Polygon.initFromExterior(gpa, &.{
        .{ .x = 90, .y = 40 }, // Inside target
        .{ .x = 110, .y = 40 }, // Outside target (extends past x=100 boundary)
        .{ .x = 110, .y = 60 }, // Outside target
        .{ .x = 90, .y = 60 }, // Inside target
        .{ .x = 90, .y = 40 },
    });
    defer cell.deinit(gpa);

    { // test against Polygon
        try expectEqual(false, target.asGeometry().touches(cell.asGeometry()));
        try expectEqual(true, target.asGeometry().intersects(cell.asGeometry()));

        var intersection = try target.asGeometry().intersection(gpa, cell.asGeometry());
        defer intersection.deinit(gpa);

        try expect(try intersection.isValid());
        try expectEqual(false, intersection.isEmpty());
        try expectEqual(true, intersection.intersects(target.asGeometry()));
        try expectEqual(false, intersection.touches(target.asGeometry()));
    }

    { // test against LinearRing. WE EXPECT THE SAME BEHAVIOR
        try expectEqual(false, target_ring.asGeometry().touches(cell.asGeometry()));
        try expectEqual(true, target_ring.asGeometry().intersects(cell.asGeometry()));

        var intersection = try target_ring.asGeometry().intersection(gpa, cell.asGeometry());
        defer intersection.deinit(gpa);

        try expect(try intersection.isValid());
        try expectEqual(false, intersection.isEmpty());
        try expectEqual(true, intersection.intersects(target_ring.asGeometry()));
        try expectEqual(false, intersection.touches(target_ring.asGeometry()));
    }
}

test "testing assumptions" {
    try init();
    defer deinit();

    const gpa = std.testing.allocator;

    var linear_ring = try LinearRing.init(gpa, &.{
        .{ .x = 0, .y = 0 },
        .{ .x = 100, .y = 0 },
        .{ .x = 100, .y = 100 },
        .{ .x = 0, .y = 100 },
        .{ .x = 0, .y = 0 },
    });
    defer linear_ring.deinit(gpa);

    var poly_inside = try Polygon.initFromExterior(gpa, &.{
        .{ .x = 10, .y = 10 },
        .{ .x = 50, .y = 10 },
        .{ .x = 50, .y = 50 },
        .{ .x = 10, .y = 50 },
        .{ .x = 10, .y = 10 },
    });
    defer poly_inside.deinit(gpa);

    try expectEqual(10, try linear_ring.asGeometry().distance(poly_inside.asGeometry()));
    try std.testing.expectError(error.InvalidGeometryTypeForArea, linear_ring.asGeometry().area());

    {
        var poly_touching = try LinearRing.init(gpa, &.{
            .{ .x = 0, .y = 0 },
            .{ .x = 50, .y = 0 },
            .{ .x = 50, .y = 50 },
            .{ .x = 0, .y = 50 },
            .{ .x = 0, .y = 0 },
        });
        defer poly_touching.deinit(gpa);

        var intersection = try linear_ring.asGeometry().intersection(gpa, poly_touching.asGeometry());
        defer intersection.deinit(gpa);

        try expect(intersection == .multilinestring);
        try expectEqual(2, intersection.multilinestring.linestrings.items.len);

        if (false) {
            for (intersection.multilinestring.linestrings.items, 0..) |ls, i| {
                std.debug.print("linestring {d}:\n", .{i});
                for (ls.coords.items) |coord| {
                    std.debug.print("{d} {d}\n", .{ coord.x, coord.y });
                }
            }
        }
    }
}
