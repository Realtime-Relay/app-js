import { describe, it, expect } from 'vitest';
import { buildCredentials, topicPatternMatcher } from '../src/utils.js';

describe('buildCredentials', () => {
    it('returns a string containing the JWT in the correct PEM block', () => {
        const result = buildCredentials('my-jwt-token', 'my-seed');
        expect(result).toContain('-----BEGIN NATS USER JWT-----');
        expect(result).toContain('my-jwt-token');
        expect(result).toContain('------END NATS USER JWT------');
    });

    it('returns a string containing the seed in the correct PEM block', () => {
        const result = buildCredentials('my-jwt-token', 'my-seed');
        expect(result).toContain('-----BEGIN USER NKEY SEED-----');
        expect(result).toContain('my-seed');
        expect(result).toContain('------END USER NKEY SEED------');
    });

    it('places the JWT between the begin and end markers', () => {
        const result = buildCredentials('JWT_ABC', 'SEED_XYZ');
        const jwtStart = result.indexOf('-----BEGIN NATS USER JWT-----');
        const jwtValue = result.indexOf('JWT_ABC');
        const jwtEnd = result.indexOf('------END NATS USER JWT------');
        expect(jwtValue).toBeGreaterThan(jwtStart);
        expect(jwtValue).toBeLessThan(jwtEnd);
    });

    it('places the seed between the begin and end markers', () => {
        const result = buildCredentials('JWT_ABC', 'SEED_XYZ');
        const seedStart = result.indexOf('-----BEGIN USER NKEY SEED-----');
        const seedValue = result.indexOf('SEED_XYZ');
        const seedEnd = result.indexOf('------END USER NKEY SEED------');
        expect(seedValue).toBeGreaterThan(seedStart);
        expect(seedValue).toBeLessThan(seedEnd);
    });

    it('includes the IMPORTANT notice about NKEYs', () => {
        const result = buildCredentials('jwt', 'seed');
        expect(result).toContain('IMPORTANT');
        expect(result).toContain('NKEY');
    });

    it('handles empty strings', () => {
        const result = buildCredentials('', '');
        expect(result).toContain('-----BEGIN NATS USER JWT-----');
        expect(result).toContain('-----BEGIN USER NKEY SEED-----');
    });
});

describe('topicPatternMatcher', () => {
    describe('literal matching', () => {
        it('matches identical single-token topics', () => {
            expect(topicPatternMatcher('foo', 'foo')).toBe(true);
        });

        it('matches identical multi-token topics', () => {
            expect(topicPatternMatcher('foo.bar.baz', 'foo.bar.baz')).toBe(true);
        });

        it('does not match different topics', () => {
            expect(topicPatternMatcher('foo', 'bar')).toBe(false);
        });

        it('does not match topics of different lengths', () => {
            expect(topicPatternMatcher('foo.bar', 'foo.bar.baz')).toBe(false);
        });

        it('does not match when one topic is a prefix of the other', () => {
            expect(topicPatternMatcher('foo.bar.baz', 'foo.bar')).toBe(false);
        });
    });

    describe('single wildcard (*)', () => {
        it('matches single token with * in first pattern', () => {
            expect(topicPatternMatcher('foo.*.baz', 'foo.bar.baz')).toBe(true);
        });

        it('matches single token with * in second pattern', () => {
            expect(topicPatternMatcher('foo.bar.baz', 'foo.*.baz')).toBe(true);
        });

        it('matches * at the beginning', () => {
            expect(topicPatternMatcher('*.bar.baz', 'foo.bar.baz')).toBe(true);
        });

        it('matches * at the end', () => {
            expect(topicPatternMatcher('foo.bar.*', 'foo.bar.baz')).toBe(true);
        });

        it('does not match multiple tokens with *', () => {
            expect(topicPatternMatcher('foo.*', 'foo.bar.baz')).toBe(false);
        });

        it('matches multiple * wildcards', () => {
            expect(topicPatternMatcher('*.*.baz', 'foo.bar.baz')).toBe(true);
        });

        it('matches * against * in other pattern', () => {
            expect(topicPatternMatcher('foo.*', 'foo.*')).toBe(true);
        });
    });

    describe('multi-token wildcard (>)', () => {
        it('matches one or more tokens with > at end of first pattern', () => {
            expect(topicPatternMatcher('foo.>', 'foo.bar')).toBe(true);
        });

        it('matches multiple trailing tokens with >', () => {
            expect(topicPatternMatcher('foo.>', 'foo.bar.baz')).toBe(true);
        });

        it('matches multiple trailing tokens with > (many tokens)', () => {
            expect(topicPatternMatcher('foo.>', 'foo.a.b.c.d')).toBe(true);
        });

        it('matches > in the second pattern', () => {
            expect(topicPatternMatcher('foo.bar.baz', 'foo.>')).toBe(true);
        });

        it('does not match > when it is not the last token', () => {
            expect(topicPatternMatcher('foo.>.bar', 'foo.x.bar')).toBe(false);
        });

        it('matches > against exactly one token', () => {
            expect(topicPatternMatcher('foo.>', 'foo.bar')).toBe(true);
        });
    });

    describe('no match cases', () => {
        it('does not match completely different topics', () => {
            expect(topicPatternMatcher('aaa.bbb', 'ccc.ddd')).toBe(false);
        });

        it('does not match when pattern is longer than topic', () => {
            expect(topicPatternMatcher('a.b.c', 'a.b')).toBe(false);
        });

        it('does not match when topic is longer than pattern', () => {
            expect(topicPatternMatcher('a.b', 'a.b.c')).toBe(false);
        });
    });

    describe('edge cases', () => {
        it('matches single token patterns', () => {
            expect(topicPatternMatcher('foo', 'foo')).toBe(true);
        });

        it('matches * against any single token', () => {
            expect(topicPatternMatcher('*', 'anything')).toBe(true);
        });

        it('matches > against single token when alone is handled', () => {
            expect(topicPatternMatcher('>', 'foo')).toBe(true);
        });

        it('matches > against multiple tokens when alone', () => {
            expect(topicPatternMatcher('>', 'foo.bar.baz')).toBe(true);
        });

        it('handles both patterns having wildcards', () => {
            expect(topicPatternMatcher('foo.*', 'foo.*')).toBe(true);
        });

        it('handles * in both patterns at different positions', () => {
            expect(topicPatternMatcher('*.bar', 'foo.*')).toBe(true);
        });

        it('matches > in both patterns', () => {
            expect(topicPatternMatcher('foo.>', 'foo.>')).toBe(true);
        });
    });
});
