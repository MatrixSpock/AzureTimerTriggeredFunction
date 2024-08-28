const { app } = require('@azure/functions');

app.timer('timerTrigger1', {
    schedule: '0 */1 * * * *',
    handler: (myTimer, context) => {
        context.log('Timer function processed request.');
        timerFunction(context, myTimer);
    }
});
