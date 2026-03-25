const IDENT_REGEX = /^[a-zA-Z0-9_-]+$/;
const HIERARCHY_REGEX = /^[a-zA-Z0-9_.\-]+$/;
const HIERARCHY_WILDCARD_REGEX = /^[a-zA-Z0-9_.*>\-]+$/;


export function validateIdent(value, fieldName) {
    if (value == null || value === '') {
        throw new Error(`${fieldName} is required`);
    }

    if (typeof value !== 'string') {
        throw new Error(`${fieldName} must be a string`);
    }

    if (!IDENT_REGEX.test(value)) {
        throw new Error(`${fieldName} contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -`);
    }
}

export function validateHierarchyName(value, fieldName) {
    if (value == null || value === '') {
        throw new Error(`${fieldName} is required`);
    }

    if (typeof value !== 'string') {
        throw new Error(`${fieldName} must be a string`);
    }

    if (!HIERARCHY_REGEX.test(value)) {
        throw new Error(`${fieldName} contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -, .`);
    }
}

export function validateHierarchyWildcard(value, fieldName) {
    if (value == null || value === '') {
        throw new Error(`${fieldName} is required`);
    }

    if (typeof value !== 'string') {
        throw new Error(`${fieldName} must be a string`);
    }

    if (!HIERARCHY_WILDCARD_REGEX.test(value)) {
        throw new Error(`${fieldName} contains invalid characters. Allowed: a-z, A-Z, 0-9, _, -, ., *, >`);
    }

    // ">" must only appear as the last token
    const tokens = value.split('.');
    const gtIndex = tokens.indexOf('>');

    if (gtIndex !== -1 && gtIndex !== tokens.length - 1) {
        throw new Error(`${fieldName} invalid: ">" can only be at the end of a topic`);
    }
}

export function validateFunction(value, fieldName) {
    if (value == null) {
        throw new Error(`${fieldName} is required`);
    }

    if (typeof value !== 'function') {
        throw new Error(`${fieldName} must be a function`);
    }
}

export function validateNonEmptyArray(value, fieldName) {
    if (!Array.isArray(value) || value.length === 0) {
        throw new Error(`${fieldName} must be a non-empty array`);
    }
}

export function validateArray(value, fieldName) {
    if (!Array.isArray(value)) {
        throw new Error(`${fieldName} must be an array`);
    }
}

export function validateObject(value, fieldName) {
    if (value == null || typeof value !== 'object' || Array.isArray(value)) {
        throw new Error(`${fieldName} must be an object`);
    }
}

export function validateConnected(connected) {
    if (!connected) {
        throw new Error('Not connected. Call app.connect() first.');
    }
}

export function validatePositiveNumber(value, fieldName) {
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
        throw new Error(`${fieldName} must be a non-negative number`);
    }
}

export function validateISO8601(value, fieldName) {
    if (value == null || value === '') {
        throw new Error(`${fieldName} is required`);
    }

    if (typeof value !== 'string') {
        throw new Error(`${fieldName} must be a string`);
    }

    const d = new Date(value);

    if (isNaN(d.getTime()) || d.toISOString() !== value) {
        throw new Error(`${fieldName} must be a valid ISO8601 datetime string`);
    }
}

export function validateStartBeforeEnd(start, end) {
    const s = new Date(start);
    const e = new Date(end);

    if (s >= e) {
        throw new Error('start must be before end');
    }
}
