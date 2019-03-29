# From https://github.com/jekyll/jekyll/pull/6362/files

require 'pathname'

module Jekyll
  module UrlRelativizer
    # Produces a path relative to the current page. For example, if the path
    # `/images/me.jpg` is supplied, then the result could be, `me.jpg`,
    # `images/me.jpg` or `../../images/me.jpg` depending on whether the current
    # page lives in `/images`, `/` or `/sub/dir/` respectively.
    #
    # input - a path relative to the project root
    #
    # Returns the supplied path relativized to the current page.
    def relativize_url(input)
      return if input.nil?
      input = input[0..0] == "/" ? input : "/#{input}"
      page_url = @context.registers[:page]["url"]
      page_dir = Pathname(page_url).parent
      Pathname(input).relative_path_from(page_dir).to_s
    end
  end
end

Liquid::Template.register_filter(Jekyll::UrlRelativizer)
