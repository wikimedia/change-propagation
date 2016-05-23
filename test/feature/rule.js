'use strict';


var assert = require('assert');
var Rule = require('../../lib/rule');


describe('Rule', function() {

    it('topic required', function() {
        assert.throws(function() {
            var r = new Rule('rule');
        }, Error, 'The rule should need to have a topic!');
    });

    it('no-op rule', function() {
        var r = new Rule('noop_rule', {topic: 'nono'});
        assert.ok(r.noop, 'The rule should be a no-op!');
    });

    it('simple rule - one request', function() {
        var r = new Rule('rule', {
            topic: 'nono',
            exec: {uri: 'a/b/c'}
        });
        assert.ok(Array.isArray(r.exec), 'exec is expected to be an array!');
    });

    it('simple rule - multiple requests', function() {
        var r = new Rule('rule', {
            topic: 'nono',
            exec: [
                {uri: 'a/b/c'},
                {uri: 'e/f/g/h'}
            ]
        });
        assert.ok(r.exec.length === 2, 'exec is expected to have 2 elements!');
    });

    describe('Matching', function() {

        var msg = {
            meta: {
                uri: 'a/fake/uri/for/you',
                request_id: '12345678-9101'
            },
            number: 1,
            string: 'oolala'
        };

        it('all', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'}
            });
            assert.ok(r.test(msg), 'Expected the rule to match all event messages!');
        });

        it('simple value match', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 1, string: 'oolala'}
            });
            assert.ok(r.test(msg), 'Expected the rule to match the given message!');
        });

        it('simple value mismatch', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 2, string: 'oolala'}
            });
            assert.ok(!r.test(msg), 'Expected the rule not to match the given message!');
        });

        it('regex match', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 1, string: '/(?:la)+/'}
            });
            assert.ok(r.test(msg), 'Expected the rule to match the given message!');
        });

        it('regex mismatch', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 1, string: '/lah/'}
            });
            assert.ok(!r.test(msg), 'Expected the rule not to match the given message!');
        });

        it('array match', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {array: ['1', 2, '/(\\d)/']}
            });
            var msg = { array: [2, '1', '3', '4', 5] };
            assert.ok(r.test(msg), 'Expected the rule to match the given message!');
        });

        it('malformed match', function() {
            assert.throws(
                function() {
                    var r = new Rule('rule', {
                        topic: 'nono',
                        exec: {uri: 'a/b/c'},
                        match: {number: 1, string: '/l\/ah/'}
                    });
            }, Error);
        });

        it('match_not', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match_not: {meta: {uri: '/my-url/'}}
            });
            assert.ok(r.test(msg), 'Expected the rule to match the given message!');
        });

        it('matches match and match_not', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 1},
                match_not: {meta: {uri: '/my-url/'}}
            });
            assert.ok(r.test(msg), 'Expected the rule to match the given message!');
        });

        it('matches match but not match_not', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 1},
                match_not: {meta: {uri: '/fake/'}}
            });
            assert.ok(!r.test(msg), 'Expected the rule not to match the given message!');
        });

        it('matches match_not but not match', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/b/c'},
                match: {number: 10},
                match_not: {meta: {uri: '/my-url/'}}
            });
            assert.ok(!r.test(msg), 'Expected the rule not to match the given message!');
        });

        it('expansion', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/{match.meta.uri[1]}/c'},
                match: { meta: { uri: "/\\/fake\\/([^\\/]+)/" }, number: 1 }
            });
            var exp = r.expand(msg);
            assert.deepEqual(exp.meta.uri, /\/fake\/([^\/]+)/.exec(msg.meta.uri));
        });

        it('expansion with named groups', function() {
            var r = new Rule('rule', {
                topic: 'nono',
                exec: {uri: 'a/{match.meta.uri.element}/c'},
                match: { meta: { uri: "/\\/fake\\/(?<element>[^\\/]+)/" }, number: 1 }
            });
            var exp = r.expand(msg);
            assert.deepEqual(exp.meta.uri, { element: 'uri' });
        });

        it('checks for named and unnamed groups mixing', function() {
            try {
                var r = new Rule('rule', {
                    topic: 'nono',
                    exec: {uri: 'a/{match.meta.uri.element}/c'},
                    match: { meta: { uri: "/\\/(\w+)\\/(?<element>[^\\/]+)/" }, number: 1 }
                });
                throw new Error('Error must be thrown');
            } catch (e) {
                assert.deepEqual(e.message,
                    'Invalid match regex. Mixing named and unnamed capture groups are not supported. Regex: /\\/(w+)\\/(?<element>[^\\/]+)/');
            }
        });

    });

});

