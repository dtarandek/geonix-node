const debug = require('debug')('service')
const nats = require('nats')
const nanoid = require('nanoid')
const hash = data => require('crypto').createHash('sha256').update(data).digest('hex')
const semver = require('semver')

module.exports = Service = class Service {

    static connection = null
    static instances = []

    static register(_options) {
        const defaults = {
            id: nanoid(),
            transport: process.env.TRANSPORT ? process.env.TRANSPORT : 'nats://127.0.0.1:4222',
            namespace: '',
            version: '1.0.0',
            service: this.name,
            stateful: false,
            registerInterval: 1000,
            callTimeout: 60 * 10000
        }

        const options = { ...defaults, ..._options }
        options.queue = hash(`${options.namespace}.${options.service}@${options.version}`)

        new this(options)
    }

    // instance methods

    readyCalled = false
    traceid = [1]

    constructor(options) {
        this.options = options

        // connect to transport
        this.transport = nats.connect({
            servers: options.transport.split(','),
            name: `${options.service}@${options.version}`.toLowerCase()
        })

        // register with broker
        const registration = {
            // "id": options.id,
            namespace: options.namespace,
            service: options.service,
            version: options.version,
            methods: {}
        }

        // parse service class and extract methods with their argument names
        const publishedMethods = Object.getOwnPropertyNames(this.constructor.prototype)
            .filter($ => $ !== 'constructor' && !$.startsWith('$'))
        for (let method of publishedMethods)
            registration.methods[method] = { params: [] }

        // bind calls
        this.transport.subscribe(`gx.${options.queue}.invoke`, { queue: options.queue }, ($, replyTo) => this.invoke(JSON.parse($), replyTo))

        // start emitting registration info
        setInterval($ => {
            this.transport.publish('gx.broker.register', JSON.stringify(registration))
        }, options.registerInterval)

        this.transport.on('connect', $ => {
            debug(`${options.service}:transport:connected`)

            // instantiate
            if (!this.readyCalled && this.onReady) {
                debug(`${this.options.service}:instance:onReady`)
                this.readyCalled = true
                this.onReady()
            }
        })
    }

    async invoke(invocation, replyTo) {
        if (!this[invocation.method])
            return this.transport.publish(replyTo, JSON.stringify({ error: 'callee:unknown_method' }))

        // this.traceid = invocation.traceid
        const result = await this[invocation.method].apply({ ...this, traceid: invocation.traceid }, invocation.args)
        // const result = await this[invocation.method].apply(this, invocation.args)

        return this.transport.publish(replyTo, JSON.stringify({ result }))
    }

    remote(identifier) {
        debug(`${this.options.service}:remote:${identifier}`)

        let [parts, version] = identifier.split('@')
        version = semver.coerce(version) ? version : '1.0.0'
        parts = parts.split('.')
        const service = parts.pop()
        const namespace = parts.join('.')
        const queue = hash(`${namespace}.${service}@${version}`)

        return new Proxy({}, {
            get: (_, method) => {
                return (...args) => {
                    return new Promise((resolve, reject) => {
                        const invocation = { namespace, service, version, method, args, traceid: this.traceid.concat([1]) }

                        if (this.traceid && this.traceid.length > 0)
                            this.traceid[this.traceid.length - 1]++

                        debug(this.traceid ? this.traceid.join('.') : '')

                        this.transport.requestOne('gx.broker.invoke', JSON.stringify(invocation), {}, this.options.callTimeout, (response) => {
                            if (response instanceof nats.NatsError && response.code === nats.REQ_TIMEOUT)
                                return reject('caller:request_timeout')

                            const parsed = JSON.parse(response)

                            if (parsed.hasOwnProperty('result'))
                                return resolve(JSON.parse(JSON.stringify(parsed.result)))
                            if (parsed.hasOwnProperty('error'))
                                return reject(parsed.error)
                        })
                    })
                }
            }
        })
    }

}
