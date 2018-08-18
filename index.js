const Storage = require('@google-cloud/storage')
const Parser = require('parse-pdf-script')
const config = require('./config')

exports.updateOppositionCollecions = (event, context) => {
  function upload2Bucket(filename, data) {
    const bucket = config.bucketJson

    filename = `${filename}.json`
    const streamAsPromise = new Promise((resolve, reject) => {
      storage
        .bucket(bucket)
        .file(filename)
        .save(JSON.stringify(data), err => {
          if (err) reject(err)
          resolve(`Upload "${filename}" succesfully!`)
        })
    })
    return streamAsPromise.then(() => {
      console.log(`Json "${filename}" parsed successfully!`)
      return null
    })
  }

  const storage = new Storage()
  const gcsEvent = event

  if (gcsEvent.name.split('.')[1] !== 'pdf') return

  let bodyparts = []

  const readStream = storage
    .bucket(gcsEvent.bucket)
    .file(gcsEvent.name)
    .createReadStream()
    .on('error', err => console.log('event: error', err.message))
    .on('data', chunk => bodyparts.push(chunk))
    .on('close', () => console.log('event: close'))

  const streamAsPromise = new Promise((resolve, reject) =>
    readStream
      .on('end', async () => {
        const result = await new Parser(gcsEvent.name, Buffer.concat(bodyparts))
        await upload2Bucket(gcsEvent.name.split('.')[0], result)
        resolve(true)
      })
      .on('error', reject)
  )

  return streamAsPromise.then(() => {
    console.log(`Filed ${gcsEvent.name} parsed successfully`)
    return null
  })
}
