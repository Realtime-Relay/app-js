export function buildCredentials(apiKey, secret) {
    return `
-----BEGIN NATS USER JWT-----
${apiKey}
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
${secret}
------END USER NKEY SEED------

*************************************************************`;
}

export function topicPatternMatcher(patternA, patternB) {
    const a = patternA.split(".");
    const b = patternB.split(".");

    let i = 0, j = 0;
    let starAi = -1, starAj = -1;
    let starBi = -1, starBj = -1;

    while (i < a.length || j < b.length) {
        const tokA = a[i];
        const tokB = b[j];

        if (tokA === ">") {
            if (i !== a.length - 1) return false;
            if (j >= b.length) return false;
            starAi = i++;
            starAj = ++j;
            continue;
        }
        if (tokB === ">") {
            if (j !== b.length - 1) return false;
            if (i >= a.length) return false;
            starBi = j++;
            starBj = ++i;
            continue;
        }

        const singleWildcard =
            (tokA === "*" && j < b.length) ||
            (tokB === "*" && i < a.length);

        if (
            (tokA !== undefined && tokA === tokB) ||
            singleWildcard
        ) {
            i++; j++;
            continue;
        }

        if (starAi !== -1) {
            j = ++starAj;
            continue;
        }
        if (starBi !== -1) {
            i = ++starBj;
            continue;
        }

        return false;
    }

    return true;
}
