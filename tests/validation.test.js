import { describe, it, expect } from 'vitest';
import {
    validateIdent,
    validateHierarchyName,
    validateHierarchyWildcard,
    validateEventName,
    validateCommandName,
    validateRpcName,
    validateTelemetryMetric,
    validateFunction,
    validateNonEmptyArray,
    validateArray,
    validateObject,
    validateConnected,
    validatePositiveNumber,
    validateISO8601,
    validateStartBeforeEnd,
} from '../src/validation.js';

describe('validateIdent', () => {
    it('accepts alphanumeric strings', () => {
        expect(() => validateIdent('abc123', 'field')).not.toThrow();
    });

    it('accepts underscores', () => {
        expect(() => validateIdent('my_ident', 'field')).not.toThrow();
    });

    it('accepts hyphens', () => {
        expect(() => validateIdent('my-ident', 'field')).not.toThrow();
    });

    it('accepts mixed valid characters', () => {
        expect(() => validateIdent('A-b_C-1', 'field')).not.toThrow();
    });

    it('rejects dots', () => {
        expect(() => validateIdent('my.ident', 'field')).toThrow(/invalid characters/);
    });

    it('rejects spaces', () => {
        expect(() => validateIdent('my ident', 'field')).toThrow(/invalid characters/);
    });

    it('rejects empty string', () => {
        expect(() => validateIdent('', 'field')).toThrow(/required/);
    });

    it('rejects null', () => {
        expect(() => validateIdent(null, 'field')).toThrow(/required/);
    });

    it('rejects undefined', () => {
        expect(() => validateIdent(undefined, 'field')).toThrow(/required/);
    });

    it('rejects non-string (number)', () => {
        expect(() => validateIdent(123, 'field')).toThrow(/must be a string/);
    });

    it('rejects non-string (boolean)', () => {
        expect(() => validateIdent(true, 'field')).toThrow(/must be a string/);
    });

    it('rejects non-string (object)', () => {
        expect(() => validateIdent({}, 'field')).toThrow(/must be a string/);
    });

    it('includes field name in error message', () => {
        expect(() => validateIdent('', 'myField')).toThrow('myField is required');
    });
});

describe('validateHierarchyName', () => {
    it('accepts alphanumeric strings', () => {
        expect(() => validateHierarchyName('abc123', 'field')).not.toThrow();
    });

    it('accepts underscores', () => {
        expect(() => validateHierarchyName('my_name', 'field')).not.toThrow();
    });

    it('accepts hyphens', () => {
        expect(() => validateHierarchyName('my-name', 'field')).not.toThrow();
    });

    it('accepts dots', () => {
        expect(() => validateHierarchyName('my.name.here', 'field')).not.toThrow();
    });

    it('accepts mixed valid characters', () => {
        expect(() => validateHierarchyName('org.my-app_v2', 'field')).not.toThrow();
    });

    it('rejects spaces', () => {
        expect(() => validateHierarchyName('my name', 'field')).toThrow(/invalid characters/);
    });

    it('rejects wildcards', () => {
        expect(() => validateHierarchyName('my.*', 'field')).toThrow(/invalid characters/);
    });

    it('rejects >', () => {
        expect(() => validateHierarchyName('my.>', 'field')).toThrow(/invalid characters/);
    });

    it('rejects empty string', () => {
        expect(() => validateHierarchyName('', 'field')).toThrow(/required/);
    });

    it('rejects null', () => {
        expect(() => validateHierarchyName(null, 'field')).toThrow(/required/);
    });

    it('rejects undefined', () => {
        expect(() => validateHierarchyName(undefined, 'field')).toThrow(/required/);
    });

    it('rejects non-string (number)', () => {
        expect(() => validateHierarchyName(42, 'field')).toThrow(/must be a string/);
    });

    it('rejects non-string (array)', () => {
        expect(() => validateHierarchyName([], 'field')).toThrow(/must be a string/);
    });
});

describe('validateHierarchyWildcard', () => {
    it('accepts alphanumeric strings', () => {
        expect(() => validateHierarchyWildcard('abc', 'field')).not.toThrow();
    });

    it('accepts underscores and hyphens', () => {
        expect(() => validateHierarchyWildcard('my_name-here', 'field')).not.toThrow();
    });

    it('accepts dots', () => {
        expect(() => validateHierarchyWildcard('a.b.c', 'field')).not.toThrow();
    });

    it('accepts single wildcard *', () => {
        expect(() => validateHierarchyWildcard('a.*.c', 'field')).not.toThrow();
    });

    it('accepts multi-token wildcard >', () => {
        expect(() => validateHierarchyWildcard('a.b.>', 'field')).not.toThrow();
    });

    it('accepts combined wildcards', () => {
        expect(() => validateHierarchyWildcard('*.b.>', 'field')).not.toThrow();
    });

    it('rejects spaces', () => {
        expect(() => validateHierarchyWildcard('a b', 'field')).toThrow(/invalid characters/);
    });

    it('rejects special characters', () => {
        expect(() => validateHierarchyWildcard('a@b', 'field')).toThrow(/invalid characters/);
    });

    it('rejects empty string', () => {
        expect(() => validateHierarchyWildcard('', 'field')).toThrow(/required/);
    });

    it('rejects null', () => {
        expect(() => validateHierarchyWildcard(null, 'field')).toThrow(/required/);
    });

    it('rejects undefined', () => {
        expect(() => validateHierarchyWildcard(undefined, 'field')).toThrow(/required/);
    });

    it('rejects non-string (number)', () => {
        expect(() => validateHierarchyWildcard(99, 'field')).toThrow(/must be a string/);
    });

    it('rejects non-string (boolean)', () => {
        expect(() => validateHierarchyWildcard(false, 'field')).toThrow(/must be a string/);
    });

    it('rejects > not at end of topic', () => {
        expect(() => validateHierarchyWildcard('a.>.b', 'field')).toThrow(/can only be at the end/);
    });

    it('rejects > in middle with more tokens', () => {
        expect(() => validateHierarchyWildcard('>.a.b', 'field')).toThrow(/can only be at the end/);
    });

    it('accepts > as only token', () => {
        expect(() => validateHierarchyWildcard('>', 'field')).not.toThrow();
    });
});

describe('validateFunction', () => {
    it('accepts a function', () => {
        expect(() => validateFunction(() => {}, 'field')).not.toThrow();
    });

    it('accepts a named function', () => {
        function myFn() {}
        expect(() => validateFunction(myFn, 'field')).not.toThrow();
    });

    it('accepts an async function', () => {
        expect(() => validateFunction(async () => {}, 'field')).not.toThrow();
    });

    it('rejects null', () => {
        expect(() => validateFunction(null, 'field')).toThrow(/required/);
    });

    it('rejects undefined', () => {
        expect(() => validateFunction(undefined, 'field')).toThrow(/required/);
    });

    it('rejects a string', () => {
        expect(() => validateFunction('notAFunction', 'field')).toThrow(/must be a function/);
    });

    it('rejects a number', () => {
        expect(() => validateFunction(42, 'field')).toThrow(/must be a function/);
    });

    it('rejects an object', () => {
        expect(() => validateFunction({}, 'field')).toThrow(/must be a function/);
    });

    it('rejects an array', () => {
        expect(() => validateFunction([], 'field')).toThrow(/must be a function/);
    });
});

describe('validateNonEmptyArray', () => {
    it('accepts a non-empty array', () => {
        expect(() => validateNonEmptyArray([1], 'field')).not.toThrow();
    });

    it('accepts an array with multiple elements', () => {
        expect(() => validateNonEmptyArray([1, 2, 3], 'field')).not.toThrow();
    });

    it('rejects an empty array', () => {
        expect(() => validateNonEmptyArray([], 'field')).toThrow(/non-empty array/);
    });

    it('rejects null', () => {
        expect(() => validateNonEmptyArray(null, 'field')).toThrow(/non-empty array/);
    });

    it('rejects undefined', () => {
        expect(() => validateNonEmptyArray(undefined, 'field')).toThrow(/non-empty array/);
    });

    it('rejects a string', () => {
        expect(() => validateNonEmptyArray('abc', 'field')).toThrow(/non-empty array/);
    });

    it('rejects an object', () => {
        expect(() => validateNonEmptyArray({}, 'field')).toThrow(/non-empty array/);
    });
});

describe('validateArray', () => {
    it('accepts an empty array', () => {
        expect(() => validateArray([], 'field')).not.toThrow();
    });

    it('accepts a non-empty array', () => {
        expect(() => validateArray([1, 2], 'field')).not.toThrow();
    });

    it('rejects null', () => {
        expect(() => validateArray(null, 'field')).toThrow(/must be an array/);
    });

    it('rejects undefined', () => {
        expect(() => validateArray(undefined, 'field')).toThrow(/must be an array/);
    });

    it('rejects a string', () => {
        expect(() => validateArray('abc', 'field')).toThrow(/must be an array/);
    });

    it('rejects a number', () => {
        expect(() => validateArray(42, 'field')).toThrow(/must be an array/);
    });

    it('rejects an object', () => {
        expect(() => validateArray({}, 'field')).toThrow(/must be an array/);
    });
});

describe('validateObject', () => {
    it('accepts a plain object', () => {
        expect(() => validateObject({}, 'field')).not.toThrow();
    });

    it('accepts an object with properties', () => {
        expect(() => validateObject({ a: 1 }, 'field')).not.toThrow();
    });

    it('rejects null', () => {
        expect(() => validateObject(null, 'field')).toThrow(/must be an object/);
    });

    it('rejects undefined', () => {
        expect(() => validateObject(undefined, 'field')).toThrow(/must be an object/);
    });

    it('rejects an array', () => {
        expect(() => validateObject([], 'field')).toThrow(/must be an object/);
    });

    it('rejects a string', () => {
        expect(() => validateObject('abc', 'field')).toThrow(/must be an object/);
    });

    it('rejects a number', () => {
        expect(() => validateObject(42, 'field')).toThrow(/must be an object/);
    });
});

describe('validateConnected', () => {
    it('does not throw when connected is true', () => {
        expect(() => validateConnected(true)).not.toThrow();
    });

    it('throws when connected is false', () => {
        expect(() => validateConnected(false)).toThrow(/Not connected/);
    });

    it('throws when connected is null', () => {
        expect(() => validateConnected(null)).toThrow(/Not connected/);
    });

    it('throws when connected is undefined', () => {
        expect(() => validateConnected(undefined)).toThrow(/Not connected/);
    });

    it('throws when connected is 0', () => {
        expect(() => validateConnected(0)).toThrow(/Not connected/);
    });

    it('throws when connected is empty string', () => {
        expect(() => validateConnected('')).toThrow(/Not connected/);
    });

    it('does not throw for truthy non-boolean values', () => {
        expect(() => validateConnected(1)).not.toThrow();
        expect(() => validateConnected('yes')).not.toThrow();
    });
});

describe('validatePositiveNumber', () => {
    it('accepts a positive integer', () => {
        expect(() => validatePositiveNumber(1, 'field')).not.toThrow();
    });

    it('accepts a positive float', () => {
        expect(() => validatePositiveNumber(0.5, 'field')).not.toThrow();
    });

    it('accepts a large number', () => {
        expect(() => validatePositiveNumber(999999, 'field')).not.toThrow();
    });

    it('accepts zero', () => {
        expect(() => validatePositiveNumber(0, 'field')).not.toThrow();
    });

    it('rejects a negative number', () => {
        expect(() => validatePositiveNumber(-1, 'field')).toThrow(/non-negative number/);
    });

    it('rejects NaN', () => {
        expect(() => validatePositiveNumber(NaN, 'field')).toThrow(/non-negative number/);
    });

    it('rejects a string', () => {
        expect(() => validatePositiveNumber('5', 'field')).toThrow(/non-negative number/);
    });

    it('rejects null', () => {
        expect(() => validatePositiveNumber(null, 'field')).toThrow(/non-negative number/);
    });

    it('rejects undefined', () => {
        expect(() => validatePositiveNumber(undefined, 'field')).toThrow(/non-negative number/);
    });
});

describe('validateISO8601', () => {
    it('accepts a valid ISO8601 date string', () => {
        expect(() => validateISO8601('2024-01-15T10:30:00.000Z', 'field')).not.toThrow();
    });

    it('rejects a date-only string (no round-trip)', () => {
        expect(() => validateISO8601('2024-01-15', 'field')).toThrow(/valid ISO8601/);
    });

    it('rejects a datetime with timezone offset (no round-trip)', () => {
        expect(() => validateISO8601('2024-01-15T10:30:00+05:30', 'field')).toThrow(/valid ISO8601/);
    });

    it('accepts a full ISO string with Z suffix', () => {
        expect(() => validateISO8601('2024-01-15T10:30:00.000Z', 'field')).not.toThrow();
    });

    it('rejects an invalid date string', () => {
        expect(() => validateISO8601('not-a-date', 'field')).toThrow(/valid ISO8601/);
    });

    it('rejects a completely nonsensical string', () => {
        expect(() => validateISO8601('hello', 'field')).toThrow(/valid ISO8601/);
    });

    it('rejects empty string', () => {
        expect(() => validateISO8601('', 'field')).toThrow(/required/);
    });

    it('rejects null', () => {
        expect(() => validateISO8601(null, 'field')).toThrow(/required/);
    });

    it('rejects undefined', () => {
        expect(() => validateISO8601(undefined, 'field')).toThrow(/required/);
    });

    it('rejects non-string (number)', () => {
        expect(() => validateISO8601(12345, 'field')).toThrow(/must be a string/);
    });
});

describe('validateStartBeforeEnd', () => {
    it('accepts when start is before end', () => {
        expect(() => validateStartBeforeEnd('2024-01-01', '2024-01-02')).not.toThrow();
    });

    it('accepts when start is well before end', () => {
        expect(() => validateStartBeforeEnd('2020-01-01T00:00:00Z', '2025-12-31T23:59:59Z')).not.toThrow();
    });

    it('throws when start equals end', () => {
        expect(() => validateStartBeforeEnd('2024-01-01T00:00:00Z', '2024-01-01T00:00:00Z')).toThrow(/start must be before end/);
    });

    it('throws when start is after end', () => {
        expect(() => validateStartBeforeEnd('2024-06-01', '2024-01-01')).toThrow(/start must be before end/);
    });

    it('works with ISO8601 datetime strings', () => {
        expect(() => validateStartBeforeEnd('2024-01-01T10:00:00Z', '2024-01-01T11:00:00Z')).not.toThrow();
    });

    it('throws when times differ by milliseconds but start >= end', () => {
        expect(() => validateStartBeforeEnd('2024-01-01T00:00:01Z', '2024-01-01T00:00:00Z')).toThrow(/start must be before end/);
    });
});

describe('validateEventName', () => {
    it('accepts valid event names', () => {
        expect(() => validateEventName('door_opened')).not.toThrow();
        expect(() => validateEventName('motion-detected')).not.toThrow();
    });

    it('rejects empty', () => {
        expect(() => validateEventName('')).toThrow(/event name is required/);
    });

    it('rejects null', () => {
        expect(() => validateEventName(null)).toThrow(/event name is required/);
    });

    it('rejects invalid characters', () => {
        expect(() => validateEventName('door.opened')).toThrow(/invalid characters/);
        expect(() => validateEventName('door opened')).toThrow(/invalid characters/);
    });
});

describe('validateCommandName', () => {
    it('accepts valid command names', () => {
        expect(() => validateCommandName('reboot')).not.toThrow();
        expect(() => validateCommandName('set_config')).not.toThrow();
    });

    it('rejects empty', () => {
        expect(() => validateCommandName('')).toThrow(/command name is required/);
    });

    it('rejects invalid characters', () => {
        expect(() => validateCommandName('set config')).toThrow(/invalid characters/);
    });
});

describe('validateRpcName', () => {
    it('accepts valid rpc names', () => {
        expect(() => validateRpcName('get_status')).not.toThrow();
        expect(() => validateRpcName('ping')).not.toThrow();
    });

    it('rejects empty', () => {
        expect(() => validateRpcName('')).toThrow(/rpc name is required/);
    });

    it('rejects invalid characters', () => {
        expect(() => validateRpcName('get status')).toThrow(/invalid characters/);
    });
});

describe('validateTelemetryMetric', () => {
    it('accepts valid metric names', () => {
        expect(() => validateTelemetryMetric('temperature')).not.toThrow();
        expect(() => validateTelemetryMetric('cpu_usage')).not.toThrow();
    });

    it('accepts wildcard', () => {
        expect(() => validateTelemetryMetric('*')).not.toThrow();
    });

    it('rejects empty', () => {
        expect(() => validateTelemetryMetric('')).toThrow(/metric is required/);
    });

    it('rejects null', () => {
        expect(() => validateTelemetryMetric(null)).toThrow(/metric is required/);
    });

    it('rejects invalid characters', () => {
        expect(() => validateTelemetryMetric('cpu.usage')).toThrow(/invalid characters/);
    });
});
