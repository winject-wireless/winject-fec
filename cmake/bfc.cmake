# bfc (header-only here: shared_sized_buffer), pinned once per build.
#
# A consumer (vstreamer, winject-l3, a firmware) that already fetched bfc sets BFC_PIN and
# BFC_INCLUDE_DIRS before adding winject-fec; its pin must match this one. A standalone build of
# this repo fetches its own pin with the default config in bfc_config/.
include_guard(GLOBAL)

set(WINJECT_FEC_BFC_PIN 804b986b10d142c2f6ed0cd93bfbfcad8c73c8a6)

if(DEFINED BFC_PIN AND NOT "${BFC_PIN}" STREQUAL "${WINJECT_FEC_BFC_PIN}")
    message(FATAL_ERROR "winject-fec pins bfc ${WINJECT_FEC_BFC_PIN}, but the build uses bfc ${BFC_PIN}. Bump one pin to match.")
endif()

if(NOT DEFINED BFC_INCLUDE_DIRS)
    if(NOT DEFINED BFC_CONFIG_DIR)
        set(BFC_CONFIG_DIR ${CMAKE_CURRENT_LIST_DIR}/../bfc_config)
    endif()
    if(NOT EXISTS ${BFC_CONFIG_DIR}/bfc_config.hpp)
        message(FATAL_ERROR "BFC_CONFIG_DIR ${BFC_CONFIG_DIR} has no bfc_config.hpp")
    endif()
    include(FetchContent)
    FetchContent_Declare(bfc
        GIT_REPOSITORY https://github.com/therooftopprinz/BFC.git
        GIT_TAG ${WINJECT_FEC_BFC_PIN}
    )
    FetchContent_GetProperties(bfc)
    if(NOT bfc_POPULATED)
        FetchContent_Populate(bfc)
    endif()
    set(BFC_INCLUDE_DIRS ${bfc_SOURCE_DIR}/src ${BFC_CONFIG_DIR})
endif()
