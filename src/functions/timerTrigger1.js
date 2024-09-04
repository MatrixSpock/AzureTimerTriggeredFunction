const { app } = require('@azure/functions');
const timerFunction = require('../index');

app.timer('timerTrigger1', {
    schedule: '0 */1 * * * *',
    handler: (myTimer, context) => {
        context.log('Timer function processed request.');
        timerFunction(context, myTimer);
    }
});
