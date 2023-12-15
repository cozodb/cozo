const webpack = require("webpack");

module.exports = {
  typescript: {
    enableTypeChecking: false,
  },
  webpack: {
    configure: (webpackConfig, { env, paths }) => {
      webpackConfig.resolve.fallback = {
        url: require.resolve("url"),
        assert: require.resolve("assert"),
        buffer: require.resolve("buffer"),
      };
      webpackConfig.module.rules.push({
        test: /\.cozo$/,
        use: "raw-loader",
      });

      webpackConfig.plugins.push(
        new webpack.ProvidePlugin({
          process: "process/browser",
          Buffer: ["buffer", "Buffer"],
        }),
        new webpack.NormalModuleReplacementPlugin(/node:/, (resource) => {
          const mod = resource.request.replace(/^node:/, "");
          switch (mod) {
            case "buffer":
              resource.request = "buffer";
              break;
            case "stream":
              resource.request = "readable-stream";
              break;
            default:
              throw new Error(`Not found ${mod}`);
          }
        })
      );

      return webpackConfig;
    },
  },
};
