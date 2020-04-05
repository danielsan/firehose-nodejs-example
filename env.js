function env (name) {
  if (!(name in process.env)) { throw new Error(`'${name}' not found in process.env`) }

  return process.env[name]
}

module.exports = env
